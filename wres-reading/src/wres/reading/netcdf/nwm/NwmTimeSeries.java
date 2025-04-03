package wres.reading.netcdf.nwm;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import wres.datamodel.types.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.space.Feature;
import wres.datamodel.time.DoubleEvent;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.PreReadException;
import wres.reading.ReadException;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Goal: a variable/feature/timeseries combination in a Vector NWM dataset is
 * considered a source.
 *
 * <p>Only the variable and NWM feature selected for an evaluation will be ingested
 * from a vector NWM dataset.
 *
 * <p>All the NWM netCDF blobs for a given timeseries will first be found and
 * opened prior to attempting to identify the timeseries, prior to attempting
 * to ingest any rows of timeseries data.
 *
 * <p>This class opens a set of NWM netCDF blobs as a timeseries, based on a profile.
 *
 * <p>This class intentionally only deals with NWM feature ids (not WRES ids).
 *
 * @author Jesse Bickel
 */

class NwmTimeSeries implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NwmTimeSeries.class );
    private static final DateTimeFormatter NWM_DATE_FORMATTER = DateTimeFormatter.ofPattern( "yyyyMMdd" );
    private static final DateTimeFormatter NWM_HOUR_FORMATTER = DateTimeFormatter.ofPattern( "HH" );

    private static final int CONCURRENT_READS = 6;

    private static final int POOL_OBJECT_LIFESPAN = 30000;
    private static final String ATTRIBUTE_FOUND_FOR_VARIABLE = "' attribute found for variable '";
    private static final String IN_NET_CDF_DATA = " in netCDF data.";
    private static final String UNABLE_TO_CONVERT_ATTRIBUTE = "Unable to convert attribute '";
    private static final String NWM_DOT = "nwm.";

    private final NwmProfile profile;

    /** The reference datetime of this NWM Forecast */
    private final Instant referenceDatetime;

    /** The reference time type. */
    private final ReferenceTimeType referenceTimeType;

    /** The base URI from where to find the members of this forecast */
    private final URI baseUri;

    /** The netCDF resources managed by this instance, opened on construction and closed on close(). */
    private final Set<NetcdfFile> netcdfFiles;

    /** The cache holding NWM feature ids for this whole set of NWM resources. */
    private final NWMFeatureCache featureCache;

    /** To parallelize requests for data from netCDF resources. */
    private final ThreadPoolExecutor readExecutor;

    /** List of features requested that were not found in this NWM TimeSeries. */
    private final Set<Long> featuresNotFound;

    /** Indicates whether no resources could be identified with data present, i.e., everything was missing. */
    @Getter
    private final boolean isAllMissing;

    @Override
    public String toString()
    {
        return "NWM Time Series with reference datetime "
               + this.getReferenceDatetime()
               + " and configuration "
               + this.getProfile().getNwmConfiguration()
               + " from base URI "
               + this.getBaseUri();
    }

    @Override
    public void close()
    {
        if ( !this.featuresNotFound.isEmpty() )
        {
            LOGGER.warn( "When reading {}, unable to find the following NWM feature id(s): {}",
                         this,
                         this.featuresNotFound );
        }

        for ( NetcdfFile netcdfFile : this.netcdfFiles )
        {
            try
            {
                netcdfFile.close();
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Could not close netCDF file {}",
                             netcdfFile,
                             ioe );
            }
        }

        this.readExecutor.shutdown();

        try
        {
            boolean result = this.readExecutor.awaitTermination( 100, TimeUnit.MILLISECONDS );
            LOGGER.debug( "The result upon awaiting termination was: {}.", result );
        }
        catch ( InterruptedException ie )
        {
            List<Runnable> abandoned = this.readExecutor.shutdownNow();
            LOGGER.warn( "{} shutdown interrupted, abandoned tasks: {}",
                         this.readExecutor,
                         abandoned,
                         ie );
            Thread.currentThread()
                  .interrupt();
        }

        if ( !this.readExecutor.isShutdown() )
        {
            List<Runnable> abandoned = this.readExecutor.shutdownNow();
            LOGGER.warn( "{} did not shut down quickly, abandoned tasks: {}",
                         this.readExecutor,
                         abandoned );
        }
    }

    /**
     * @param profile the profile
     * @param referenceDatetime the reference time
     * @param baseUri the base uri
     * @throws NullPointerException When any argument is null.
     * @throws ReadException When any netCDF blob could not be opened.
     * @throws IllegalArgumentException When baseUri is not absolute.
     */

    NwmTimeSeries( NwmProfile profile,
                   Instant referenceDatetime,
                   ReferenceTimeType referenceTimeType,
                   URI baseUri )
    {
        Objects.requireNonNull( profile );
        Objects.requireNonNull( referenceDatetime );
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( referenceTimeType );

        this.profile = profile;
        this.referenceDatetime = referenceDatetime;
        this.referenceTimeType = referenceTimeType;

        // Require an absolute URI
        if ( baseUri.isAbsolute() )
        {
            this.baseUri = baseUri;
        }
        else
        {
            throw new IllegalArgumentException( "baseUri must be absolute, not "
                                                + baseUri );
        }

        // Build the set of URIs based on the profile given.
        Set<URI> netcdfUris = NwmTimeSeries.getNetcdfUris( profile,
                                                           referenceDatetime,
                                                           this.baseUri );

        LOGGER.debug( "Created a NWM time-series reader with these {} URIs to read: {}.",
                      netcdfUris.size(),
                      netcdfUris );

        this.netcdfFiles = new HashSet<>( netcdfUris.size() );
        LOGGER.debug( "Attempting to open NWM TimeSeries with reference datetime {} and profile {} from baseUri {}.",
                      referenceDatetime,
                      profile,
                      this.baseUri );

        ThreadFactory nwmReaderThreadFactory = new BasicThreadFactory.Builder()
                .namingPattern( "NwmTimeSeries Reader %d" )
                .build();

        BlockingQueue<Runnable> nwmReaderQueue =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        this.readExecutor = new ThreadPoolExecutor( CONCURRENT_READS,
                                                    CONCURRENT_READS,
                                                    POOL_OBJECT_LIFESPAN,
                                                    TimeUnit.MILLISECONDS,
                                                    nwmReaderQueue,
                                                    nwmReaderThreadFactory );
        this.readExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );

        this.featuresNotFound = new HashSet<>( 1 );

        // Open all the relevant files during construction, or fail.
        Set<URI> resourcesNotFound = this.openNetcdfFilesForReading( netcdfUris, this.readExecutor );

        this.featureCache = new NWMFeatureCache( this.netcdfFiles );

        // Nothing missing
        if ( netcdfUris.size() == this.netcdfFiles.size() )
        {
            LOGGER.debug( "Successfully opened NWM TimeSeries with reference datetime {} and profile {} from {}.",
                          referenceDatetime,
                          profile,
                          this.baseUri );
        }
        // Something missing
        else if ( LOGGER.isWarnEnabled() )
        {
            // Everything missing
            if ( this.netcdfFiles.isEmpty() )
            {
                LOGGER.warn( "Skipping NWM TimeSeries (not found) with reference datetime {} and profile {} from {}. "
                             + "No netCDF resources were found.",
                             referenceDatetime,
                             profile,
                             this.baseUri );
            }
            // Something missing
            else
            {
                LOGGER.warn( "Found a partial NWM TimeSeries with reference datetime {} and profile {} from {}."
                             + "The following resources were not found: {}.",
                             referenceDatetime,
                             profile,
                             this.baseUri,
                             resourcesNotFound );
            }
        }

        // If everything is missing, flag and expose so that caller can track/handle
        this.isAllMissing = this.netcdfFiles.isEmpty();
    }

    /**
     * Create the Set of URIs for the whole forecast based on given nwm profile.
     * <br />
     * Assumes:
     * <ol>
     *     <li>NWM emits a regular timeseries using a single timestep.</li>
     *     <li>The first value in a timeseries is one timestep after reference
     *         date.</li>
     * </ol>
     * @param profile The metadata describing the NWM timeseries(es).
     * @param referenceDatetime The reference datetime for the forecast set.
     * @param baseUri The file or network protocol and path prefix.
     * @return The full Set of URIs for a single forecast
     */

    static Set<URI> getNetcdfUris( NwmProfile profile,
                                   Instant referenceDatetime,
                                   URI baseUri )
    {
        LOGGER.debug( "Called getNetcdfUris with {}, {}, {}",
                      profile,
                      referenceDatetime,
                      baseUri );
        Set<URI> uris = new TreeSet<>();

        // Formatter cannot handle Instant
        OffsetDateTime referenceOffsetDateTime = OffsetDateTime.ofInstant( referenceDatetime,
                                                                           ZoneId.of( "UTC" ) );
        String nwmDatePath = NWM_DOT
                             + NWM_DATE_FORMATTER.format( referenceOffsetDateTime );

        // Append a path separator to the base URI if one does not exist: #100561
        String baseUriString = baseUri.toString();
        if ( !baseUriString.endsWith( "/" )
             && !baseUriString.endsWith( "\\" ) )
        {
            baseUri = URI.create( baseUriString.concat( "/" ) );
        }

        URI uriWithDate = baseUri.resolve( nwmDatePath + "/" );

        for ( short i = 1; i <= profile.getMemberCount(); i++ )
        {
            String directoryName = profile.getNwmSubdirectoryPrefix();

            if ( profile.isEnsembleLike()
                 && NwmTimeSeries.isNotLegacyNwmVersion( baseUri ) ) // Yuck: #110992
            {
                directoryName += "_mem" + i;
            }

            URI uriWithDirectory = uriWithDate.resolve( directoryName + "/" );

            for ( short j = 1; j <= profile.getBlobCount(); j++ )
            {
                URI fullUri =
                        NwmTimeSeries.getNetcdfUri( profile, baseUri, uriWithDirectory, referenceOffsetDateTime, i, j );
                uris.add( fullUri );
            }
        }

        LOGGER.debug( "Returning these netCDF URIs: {}", uris );
        return Collections.unmodifiableSet( uris );
    }

    NwmProfile getProfile()
    {
        return this.profile;
    }

    int countOfNetcdfFiles()
    {
        return this.getNetcdfFiles().size();
    }

    /**
     * Read the first value for a given variable name attribute from the netCDF
     * files.
     *
     * @param variableName The NWM variable name.
     * @return The String representation of the value of attribute of variable.
     * @throws IllegalStateException if the NetCDF data could not be accessed
     */

    String readAttributeAsString( String variableName )
    {
        if ( !this.getNetcdfFiles()
                  .isEmpty() )
        {
            // Use the very first netcdf file, assume homogeneity.
            Variable variableVariable;
            NetcdfFile netcdfFile = this.getNetcdfFiles()
                                        .iterator()
                                        .next();  // Do not close here
            variableVariable = netcdfFile.findVariable( variableName );

            if ( variableVariable == null )
            {
                Set<String> variables = netcdfFile.getVariables()
                                                  .stream()
                                                  .map( Variable::getFullName )
                                                  .collect( Collectors.toSet() );

                // Remove the metadata variables
                variables.remove( "time" );
                variables.remove( "reference_time" );
                variables.remove( "feature_id" );
                variables.remove( "crs" );

                throw new IllegalArgumentException( "There was no variable '"
                                                    + variableName
                                                    + "' in the netCDF blob at '"
                                                    + netcdfFile.getLocation()
                                                    + "'. The blob contained the following readable variables: "
                                                    + variables
                                                    + ". Please declare one of these case-sensitive variable names to "
                                                    + "evaluate." );
            }

            return NwmTimeSeries.readAttributeAsString( variableVariable );
        }
        else
        {
            throw new IllegalStateException( "No NetCDF data available." );
        }
    }

    /**
     * Read TimeSerieses from across several netCDF single-valid datetime files.
     * @param featureIds The NWM feature IDs to read.
     * @param variableName The NWM variable name.
     * @param unitName The unit of all variable values.
     * @return a map of feature id to TimeSeries containing the events, may be
     * empty when no feature ids given were found in the NWM Data.
     */

    Map<Long, TimeSeries<Double>> readSingleValuedTimeSerieses( long[] featureIds,
                                                                String variableName,
                                                                String unitName )
            throws InterruptedException, ExecutionException
    {
        // Check that the executor is still open
        if ( this.readExecutor.isShutdown() )
        {
            throw new ReadException( "Cannot read from this NWM time-series because it has been closed: " + this );
        }

        BlockingQueue<Future<NWMDoubleReadOutcome>> reads =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        CountDownLatch startGettingResults = new CountDownLatch( CONCURRENT_READS );
        Map<Long, SortedSet<Event<Double>>> events = new HashMap<>( featureIds.length );

        for ( long featureId : featureIds )
        {
            SortedSet<Event<Double>> emptyList = new TreeSet<>();
            events.put( featureId, emptyList );
        }

        for ( NetcdfFile netcdfFile : this.getNetcdfFiles() )
        {
            NWMDoubleReader reader = new NWMDoubleReader( this.getProfile(),
                                                          netcdfFile,
                                                          featureIds.clone(),
                                                          variableName,
                                                          this.getReferenceDatetime(),
                                                          this.featureCache,
                                                          false );

            Future<NWMDoubleReadOutcome> future = this.readExecutor.submit( reader );
            reads.add( future );

            startGettingResults.countDown();

            if ( startGettingResults.getCount() <= 0 )
            {
                NWMDoubleReadOutcome outcome = reads.take()
                                                    .get();
                List<EventForNWMFeature<Double>> read = outcome.data();
                this.featuresNotFound.addAll( outcome.featuresNotFound() );

                for ( EventForNWMFeature<Double> event : read )
                {
                    // The reads are across features, we want data by feature.
                    SortedSet<Event<Double>> sortedEvents = events.get( event.getFeatureId() );
                    sortedEvents.add( event.getEvent() );
                }
            }
        }

        // Finish getting the remainder of events being read.
        for ( Future<NWMDoubleReadOutcome> reading : reads )
        {
            NWMDoubleReadOutcome outcome = reading.get();
            List<EventForNWMFeature<Double>> read = outcome.data();
            this.featuresNotFound.addAll( outcome.featuresNotFound() );

            for ( EventForNWMFeature<Double> event : read )
            {
                // The reads are across features, we want data by feature.
                SortedSet<Event<Double>> sortedEvents = events.get( event.getFeatureId() );
                sortedEvents.add( event.getEvent() );
            }
        }

        // Go back and remove all the entries for non-existent data
        for ( Long notFoundFeature : this.featuresNotFound )
        {
            events.remove( notFoundFeature );
        }

        Map<Long, TimeSeries<Double>> allTimeSerieses = new HashMap<>( featureIds.length );

        // Create each TimeSeries
        for ( Map.Entry<Long, SortedSet<Event<Double>>> series : events.entrySet() )
        {
            // TODO: use the reference datetime from actual data, not args.
            // The datetimes seem to be synchronized but this is not true for
            // analyses.
            Geometry geometry = MessageUtilities.getGeometry(
                    series.getKey()
                          .toString() );
            Feature feature = Feature.of( geometry );

            TimeSeriesMetadata metadata =
                    TimeSeriesMetadata.of( Map.of( this.getReferenceTimeType(), this.getReferenceDatetime() ),
                                           null,
                                           variableName,
                                           feature,
                                           unitName );
            TimeSeries<Double> timeSeries = TimeSeries.of( metadata,
                                                           series.getValue() );
            allTimeSerieses.put( series.getKey(), timeSeries );
        }

        return Collections.unmodifiableMap( allTimeSerieses );
    }

    Map<Long, TimeSeries<Ensemble>> readEnsembleTimeSerieses( long[] featureIds,
                                                              String variableName,
                                                              String unitName )
            throws InterruptedException, ExecutionException
    {
        // Check that the executor is still open
        if ( this.readExecutor.isShutdown() )
        {
            throw new ReadException( "Cannot read from this NWM time-series because it has been closed: " + this );
        }

        int memberCount = this.getProfile().getMemberCount();
        int validDatetimeCount = this.getProfile().getBlobCount();

        // Map of nwm feature id to a map of each timestep with member values.
        Map<Long, Map<Instant, double[]>> ensembleValues = new HashMap<>( featureIds.length );

        BlockingQueue<Future<NWMDoubleReadOutcome>> reads =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        CountDownLatch startGettingResults = new CountDownLatch(
                CONCURRENT_READS );

        for ( NetcdfFile netcdfFile : this.getNetcdfFiles() )
        {
            NWMDoubleReader reader = new NWMDoubleReader( this.getProfile(),
                                                          netcdfFile,
                                                          featureIds,
                                                          variableName,
                                                          this.getReferenceDatetime(),
                                                          this.featureCache,
                                                          true );
            Future<NWMDoubleReadOutcome> future =
                    this.readExecutor.submit( reader );
            reads.add( future );

            startGettingResults.countDown();

            if ( startGettingResults.getCount() <= 0 )
            {
                NWMDoubleReadOutcome outcome = reads.take()
                                                    .get();
                List<EventForNWMFeature<Double>> read = outcome.data();
                this.featuresNotFound.addAll( outcome.featuresNotFound() );
                this.putEnsembleDataInMap( read,
                                           ensembleValues,
                                           memberCount,
                                           validDatetimeCount );
            }
        }

        // Finish getting the remainder of events being read.
        for ( Future<NWMDoubleReadOutcome> reading : reads )
        {
            NWMDoubleReadOutcome outcome = reading.get();
            List<EventForNWMFeature<Double>> read = outcome.data();
            this.featuresNotFound.addAll( outcome.featuresNotFound() );
            this.putEnsembleDataInMap( read,
                                       ensembleValues,
                                       memberCount,
                                       validDatetimeCount );
        }

        Map<Long, TimeSeries<Ensemble>> byFeatureId = new HashMap<>( featureIds.length
                                                                     - this.featuresNotFound.size() );

        // For each feature, create a TimeSeries.
        for ( Map.Entry<Long, Map<Instant, double[]>> entriesForOne : ensembleValues.entrySet() )
        {
            SortedSet<Event<Ensemble>> sortedEvents = new TreeSet<>();

            // For each ensemble row within a feature, add values to SortedSet.
            for ( Map.Entry<Instant, double[]> entry : entriesForOne.getValue()
                                                                    .entrySet() )
            {
                Ensemble ensemble = Ensemble.of( entry.getValue() );
                Event<Ensemble> ensembleEvent =
                        Event.of( entry.getKey(), ensemble );
                sortedEvents.add( ensembleEvent );
            }

            Geometry geometry = MessageUtilities.getGeometry(
                    entriesForOne.getKey()
                                 .toString() );
            Feature feature = Feature.of( geometry );

            TimeSeriesMetadata metadata =
                    TimeSeriesMetadata.of( Map.of( this.getReferenceTimeType(), this.getReferenceDatetime() ),
                                           null,
                                           variableName,
                                           feature,
                                           unitName );
            // Create the TimeSeries for the current Feature
            TimeSeries<Ensemble> timeSeries = TimeSeries.of( metadata,
                                                             sortedEvents );

            // Store this TimeSeries in the collection to be returned.
            byFeatureId.put( entriesForOne.getKey(), timeSeries );
        }

        return Collections.unmodifiableMap( byFeatureId );
    }

    /**
     * Inspects a NWM URI and attempts to determine whether it is a "legacy" NWM version, defined as 1.1 or 1.2, since
     * these are supported versions that have a different file structure. Yuck, but better than adding yet more
     * interfaces for datasets that are (increasingly) rarely used. See #110992.
     *
     * @param baseUri the base URI to check
     * @return whether the path is consistent with a legacy 1.1 or 1.2 model version
     */

    private static boolean isNotLegacyNwmVersion( URI baseUri )
    {
        return !baseUri.getPath()
                       .contains( "1.1" )
               && !baseUri.getPath()
                          .contains( "1.2" );
    }

    /**
     * Returns a NetCDF URI from the inputs.
     * @param profile the profile
     * @param baseUri the base URI
     * @param uriWithDirectory the URI with the directory
     * @param referenceOffsetDateTime the offset reference time
     * @param memberIndex the member index
     * @param blobIndex the blob index
     * @return the URI
     */

    private static URI getNetcdfUri( NwmProfile profile,
                                     URI baseUri,
                                     URI uriWithDirectory,
                                     OffsetDateTime referenceOffsetDateTime,
                                     int memberIndex,
                                     int blobIndex )
    {
        String ncFilePartOne = NWM_DOT + "t"
                               + NWM_HOUR_FORMATTER.format( referenceOffsetDateTime )
                               + "z."
                               + profile.getNwmConfiguration()
                               + "."
                               + profile.getNwmOutputType();

        // Ensemble number appended if greater than one member present.
        if ( profile.isEnsembleLike()
             && NwmTimeSeries.isNotLegacyNwmVersion( baseUri ) ) // Yuck: #110992
        {
            ncFilePartOne += "_" + memberIndex;
        }

        String ncFilePartTwo = "." + profile.getTimeLabel();

        Duration validDatetimeStep = profile.getDurationBetweenValidDatetimes();
        Duration oneHour = Duration.ofHours( 1 );
        boolean subHourly = validDatetimeStep.compareTo( oneHour ) < 0;
        Duration duration = validDatetimeStep.multipliedBy( blobIndex );
        long hours = duration.toHours();

        if ( profile.getTimeLabel()
                    .equals( NwmProfile.TimeLabel.F ) )
        {
            if ( !subHourly )
            {
                String forecastLabel = String.format( "%03d", hours );
                ncFilePartTwo += forecastLabel;
            }
            else
            {
                // More-frequent-than-hourly means use 3-digit hour then
                // two digit minute
                long minutes = duration.minusHours( hours )
                                       .toMinutes();
                String forecastLabel = String.format( "%03d%02d",
                                                      hours,
                                                      minutes );
                ncFilePartTwo += forecastLabel;
            }
        }
        else if ( profile.getTimeLabel()
                         .equals( NwmProfile.TimeLabel.TM ) )
        {
            if ( !subHourly )
            {
                long hourLabel = duration.minus( validDatetimeStep )
                                         .toHours();
                // Analysis files go back in valid datetime as j increases.
                String analysisLabel = String.format( "%02d", hourLabel );
                ncFilePartTwo += analysisLabel;
            }
            else
            {
                long analysisHour = duration.minus( validDatetimeStep )
                                            .toHours();
                long analysisMinute = duration.minusHours( hours )
                                              .toMinutes();
                String analysisLabel = String.format( "%02d%02d",
                                                      analysisHour,
                                                      analysisMinute );
                ncFilePartTwo += analysisLabel;
            }
        }

        String ncFilePartThree = "." + profile.getNwmLocationLabel()
                                 + ".nc";
        String ncFile = ncFilePartOne + ncFilePartTwo + ncFilePartThree;
        LOGGER.trace( "Built a netCDF filename: {}", ncFile );

        return NwmTimeSeries.getUriForScheme( uriWithDirectory, ncFile );
    }

    private Instant getReferenceDatetime()
    {
        return this.referenceDatetime;
    }

    private ReferenceTimeType getReferenceTimeType()
    {
        return this.referenceTimeType;
    }

    private URI getBaseUri()
    {
        return this.baseUri;
    }

    private Set<NetcdfFile> getNetcdfFiles()
    {
        return this.netcdfFiles;
    }

    /**
     * Opens the supplied NetCDF URIs, reading for reading.
     *
     * @param netcdfUris the uris to NetCDF blobs
     * @param readExecutor the read executor
     * @return any resources not found
     */

    private Set<URI> openNetcdfFilesForReading( Set<URI> netcdfUris,
                                                ThreadPoolExecutor readExecutor )
    {
        BlockingQueue<Pair<URI, Future<NetcdfFile>>> openBlobQueue =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        CountDownLatch startGettingResults =
                new CountDownLatch( CONCURRENT_READS );

        Set<URI> resourcesNotFound = new HashSet<>();
        for ( URI netcdfUri : netcdfUris )
        {
            NWMResourceOpener opener = new NWMResourceOpener( netcdfUri );
            Future<NetcdfFile> futureBlob = readExecutor.submit( opener );
            Pair<URI, Future<NetcdfFile>> futureBlobPair = Pair.of( netcdfUri, futureBlob );
            openBlobQueue.add( futureBlobPair );
            startGettingResults.countDown();

            if ( startGettingResults.getCount() <= 0 )
            {
                try
                {
                    NetcdfFile netcdfFile = openBlobQueue.take()
                                                         .getRight()
                                                         .get();
                    this.netcdfFiles.add( netcdfFile );
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.warn( "Interrupted while opening netCDF resources.", ie );
                    this.close();
                    Thread.currentThread().interrupt();
                }
                catch ( ExecutionException ee )
                {
                    this.allowFileNotFoundOrThrowReadException( netcdfUri, ee, resourcesNotFound );
                }
            }
        }

        // Finish getting the remainder of netCDF resources being opened.
        for ( Pair<URI, Future<NetcdfFile>> opening : openBlobQueue )
        {
            try
            {
                NetcdfFile netcdfFile = opening.getRight()
                                               .get();
                this.netcdfFiles.add( netcdfFile );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while opening netCDF resources.", ie );
                this.close();
                Thread.currentThread()
                      .interrupt();
            }
            catch ( ExecutionException ee )
            {
                this.allowFileNotFoundOrThrowReadException( opening.getLeft(), ee, resourcesNotFound );
            }
        }

        return Collections.unmodifiableSet( resourcesNotFound );
    }

    /**
     * Inspects the exception and, if the cause of the exception is a {@link FileNotFoundException}, adds it to the
     * supplied set, otherwise throws an {@link ReadException} indicating that the resource could not be read.
     * @param uri the URI that was attempted
     * @param e the exception to inspect
     * @param resourcesNotFound the set of resources not found
     * @throws ReadException if the cause of the input exception is not a {@link FileNotFoundException}
     */

    private void allowFileNotFoundOrThrowReadException( URI uri, ExecutionException e, Set<URI> resourcesNotFound )
    {
        Throwable cause = e.getCause();

        if ( Objects.nonNull( cause )
             && cause instanceof FileNotFoundException )
        {
            LOGGER.debug( "Failed to open NetCDF resource, '{}', because it could not be found: {}",
                          uri, e.getMessage() );
            resourcesNotFound.add( uri );
        }
        else
        {
            this.close();
            throw new ReadException( "Could not open a NetCDF resource to read time-series data from the National "
                                     + "Water Model. The following resource was attempted: " + uri, e );
        }
    }

    /**
     * Read data into intermediate format more convenient for wres.datamodel.
     * @param events Reads this data to put into ensembleValues.
     * @param ensembleValues MUTATES this data using data found from events.
     * @param memberCount The count of ensemble members.
     * @param validDatetimeCount The count of validDatetimes.
     */
    private void putEnsembleDataInMap( List<EventForNWMFeature<Double>> events,
                                       Map<Long, Map<Instant, double[]>> ensembleValues,
                                       int memberCount,
                                       int validDatetimeCount )
    {

        for ( EventForNWMFeature<Double> event : events )
        {
            Instant eventDatetime = event.getEvent()
                                         .getTime();
            Long featureId = event.getFeatureId();
            Map<Instant, double[]> fullEnsemble = ensembleValues.get( featureId );

            if ( Objects.isNull( fullEnsemble ) )
            {
                fullEnsemble = new HashMap<>( validDatetimeCount );
                ensembleValues.put( featureId, fullEnsemble );
            }

            double[] ensembleRow = fullEnsemble.get( eventDatetime );

            if ( Objects.isNull( ensembleRow ) )
            {
                ensembleRow = new double[memberCount];

                // Fill new row with no-data-values to tolerate missing data.
                // There may be a better way. See #73944
                Arrays.fill( ensembleRow, MissingValues.DOUBLE );
                fullEnsemble.put( eventDatetime, ensembleRow );
            }

            int ncEnsembleNumber = event.getEnsembleMemberNumber();
            ensembleRow[ncEnsembleNumber - 1] = event.getEvent()
                                                     .getValue();
        }
    }

    private static DataType getAttributeType( Variable ncVariable,
                                              String attributeName )
    {
        for ( Attribute attribute : ncVariable.attributes() )
        {
            if ( attribute.getName()
                          .equalsIgnoreCase( attributeName.toLowerCase() ) )
            {
                return attribute.getDataType();
            }
        }

        throw new IllegalStateException( "No '" + attributeName
                                         + ATTRIBUTE_FOUND_FOR_VARIABLE
                                         + ncVariable
                                         + IN_NET_CDF_DATA );
    }

    /**
     * @param ncVariable The NWM variable.
     * @return The String representation of the value of attribute of variable.
     */

    private static String readAttributeAsString( Variable ncVariable )
    {
        for ( Attribute attribute : ncVariable.attributes() )
        {
            if ( attribute.getName()
                          .equalsIgnoreCase( "units" ) )
            {
                return attribute.getStringValue();
            }
        }

        throw new IllegalStateException( "No 'units"
                                         + ATTRIBUTE_FOUND_FOR_VARIABLE
                                         + ncVariable
                                         + IN_NET_CDF_DATA );
    }


    /**
     * @param ncVariable The NWM variable.
     * @param attributeName The attribute associated with the variable.
     * @return The double representation of the value of attribute of variable.
     * @throws IllegalArgumentException When the attribute does not exist.
     * @throws CastMayCauseBadConversionException When the type is not double.
     */

    private static double readAttributeAsDouble( Variable ncVariable,
                                                 String attributeName )
    {
        for ( Attribute attribute : ncVariable.attributes() )
        {
            if ( attribute.getName()
                          .equalsIgnoreCase( attributeName.toLowerCase() ) )
            {
                DataType type = attribute.getDataType();

                if ( type.equals( DataType.DOUBLE ) )
                {
                    return Objects.requireNonNull( attribute.getNumericValue() )
                                  .doubleValue();
                }
                else
                {
                    throw new CastMayCauseBadConversionException( UNABLE_TO_CONVERT_ATTRIBUTE
                                                                  + attributeName
                                                                  + "' to double because it is type "
                                                                  + type );
                }
            }
        }

        throw new IllegalArgumentException( "No '" + attributeName
                                            + ATTRIBUTE_FOUND_FOR_VARIABLE
                                            + ncVariable
                                            + IN_NET_CDF_DATA );
    }

    /**
     * @param ncVariable The NWM variable.
     * @param attributeName The attribute associated with the variable.
     * @return The float representation of the value of attribute of variable.
     * @throws IllegalArgumentException When the attribute does not exist.
     * @throws CastMayCauseBadConversionException When the type is not float.
     */

    private static float readAttributeAsFloat( Variable ncVariable,
                                               String attributeName )
    {
        for ( Attribute attribute : ncVariable.attributes() )
        {
            if ( attribute.getName()
                          .equalsIgnoreCase( attributeName.toLowerCase() ) )
            {
                DataType type = attribute.getDataType();

                if ( type.equals( DataType.FLOAT ) )
                {
                    return Objects.requireNonNull( attribute.getNumericValue() )
                                  .floatValue();
                }
                else
                {
                    throw new CastMayCauseBadConversionException(
                            UNABLE_TO_CONVERT_ATTRIBUTE
                            + attributeName
                            + "' to float because it is type "
                            + type );
                }
            }
        }

        throw new IllegalArgumentException( "No '" + attributeName
                                            + ATTRIBUTE_FOUND_FOR_VARIABLE
                                            + ncVariable
                                            + IN_NET_CDF_DATA );
    }


    /**
     * @param ncVariable The NWM variable.
     * @param attributeName The attribute associated with the variable.
     * @return The String representation of the value of attribute of variable.
     * @throws IllegalArgumentException When the attribute does not exist.
     * @throws CastMayCauseBadConversionException When the type cast would cause loss.
     */

    private static int readAttributeAsInt( Variable ncVariable,
                                           String attributeName )
    {
        for ( Attribute attribute : ncVariable.attributes() )
        {
            if ( attribute.getName()
                          .equalsIgnoreCase( attributeName.toLowerCase() ) )
            {
                DataType type = attribute.getDataType();

                if ( type.equals( DataType.BYTE )
                     || type.equals( DataType.UBYTE )
                     || type.equals( DataType.SHORT )
                     || type.equals( DataType.USHORT )
                     || type.equals( DataType.INT ) )
                {
                    // No loss of precision nor out of bounds possibility when
                    // promoting byte, ubyte, short, ushort, int to int.
                    return Objects.requireNonNull( attribute.getNumericValue() )
                                  .intValue();
                }
                else
                {
                    throw new CastMayCauseBadConversionException(
                            UNABLE_TO_CONVERT_ATTRIBUTE
                            + attributeName
                            + "' to integer because it is type "
                            + type );
                }
            }
        }

        throw new IllegalArgumentException( "No '" + attributeName
                                            + ATTRIBUTE_FOUND_FOR_VARIABLE
                                            + ncVariable
                                            + IN_NET_CDF_DATA );
    }

    private static Instant readValidDatetime( NwmProfile profile,
                                              NetcdfFile netcdfFile )
    {
        String validDatetimeVariableName = profile.getValidDatetimeVariable();
        return NwmTimeSeries.readMinutesFromEpoch( netcdfFile,
                                                   validDatetimeVariableName );
    }

    private static Instant readReferenceDatetime( NwmProfile profile,
                                                  NetcdfFile netcdfFile )
    {
        String referenceDatetimeAttributeName = profile.getReferenceDatetimeVariable();
        return NwmTimeSeries.readMinutesFromEpoch( netcdfFile,
                                                   referenceDatetimeAttributeName );
    }

    private static VariableAttributes readVariableAttributes( Variable variable )
    {
        final String MISSING_VALUE_NAME = "missing_value";
        final String FILL_VALUE_NAME = "_FillValue";
        final String SCALE_FACTOR_NAME = "scale_factor";
        final String OFFSET_NAME = "add_offset";

        int missingValue = readAttributeAsInt( variable,
                                               MISSING_VALUE_NAME );
        int fillValue = readAttributeAsInt( variable,
                                            FILL_VALUE_NAME );
        boolean has32BitPacking;
        float multiplier32 = Float.NaN;
        float offsetToAdd32 = Float.NaN;
        double multiplier64 = Double.NaN;
        double offsetToAdd64 = Double.NaN;

        DataType multiplierType = getAttributeType( variable,
                                                    SCALE_FACTOR_NAME );
        DataType offsetType = getAttributeType( variable,
                                                OFFSET_NAME );

        if ( !multiplierType.equals( offsetType ) )
        {
            throw new UnsupportedOperationException( "The variable "
                                                     + variable
                                                     + " has inconsistent types"
                                                     + " for attributes '"
                                                     + SCALE_FACTOR_NAME
                                                     + "' and '"
                                                     + OFFSET_NAME
                                                     + "': '"
                                                     + multiplierType
                                                     + "' and '"
                                                     + offsetType
                                                     + "' respectively. The CF "
                                                     + "conventions on packing "
                                                     + "disallow this." );
        }

        if ( multiplierType.equals( DataType.FLOAT ) )
        {
            has32BitPacking = true;
            multiplier32 = readAttributeAsFloat( variable, SCALE_FACTOR_NAME );
            offsetToAdd32 = readAttributeAsFloat( variable, OFFSET_NAME );
        }
        else if ( multiplierType.equals( DataType.DOUBLE ) )
        {
            has32BitPacking = false;
            multiplier64 = readAttributeAsDouble( variable, SCALE_FACTOR_NAME );
            offsetToAdd64 = readAttributeAsDouble( variable, OFFSET_NAME );
        }
        else
        {
            throw new UnsupportedOperationException( "Only 32-bit (float) and"
                                                     + "64-bit (double) "
                                                     + "floating point packing "
                                                     + "is supported in this "
                                                     + "version of WRES." );
        }

        return new VariableAttributes( missingValue,
                                       fillValue,
                                       has32BitPacking,
                                       multiplier32,
                                       offsetToAdd32,
                                       multiplier64,
                                       offsetToAdd64 );
    }


    /**
     * <p>ctually read nc data from a variable, targeted to given indices.
     *
     * <p>It is OK for indices to be unsorted, e.g. for minIndex to appear anywhere
     * in indices and maxIndex to appear anywhere in indices.
     *
     * <p>No value in indices passed may be negative.
     *
     * @param variable The variable to read data from.
     * @param indices The indices to read.
     * @param minIndex Caller-supplied minimum from indices (avoid searching 2x)
     * @param maxIndex Caller-supplied maximum from indices (avoid searching 2x)
     * @return An int[] with same cardinality and order as indices argument.
     */
    private static int[] readRawInts( Variable variable,
                                      int[] indices,
                                      int minIndex,
                                      int maxIndex )
    {
        Objects.requireNonNull( variable );
        Objects.requireNonNull( indices );

        String variableName = variable.getFullName();

        if ( indices.length < 1 )
        {
            throw new IllegalArgumentException( "Must pass at least one index." );
        }

        if ( minIndex < 0 )
        {
            throw new IllegalArgumentException( "minIndex must be positive." );
        }

        if ( maxIndex < 0 )
        {
            throw new IllegalArgumentException( "maxIndex must be positive." );
        }

        if ( minIndex > maxIndex )
        {
            throw new IllegalArgumentException( "maxIndex must be >= minIndex." );
        }

        int[] result = new int[indices.length];
        int countOfRawValuesToRead = maxIndex - minIndex + 1;
        int[] rawValues;
        int[] origin = { minIndex };
        int[] shape = { countOfRawValuesToRead };

        try
        {
            Array array = variable.read( origin, shape );
            int[] unsafeValues = ( int[] ) array.get1DJavaArray( DataType.INT );
            rawValues = unsafeValues.clone();
        }
        catch ( IOException | InvalidRangeException e )
        {
            throw new PreReadException( "Failed to read variable "
                                        + variableName
                                        + " at origin "
                                        + Arrays.toString( origin )
                                        + " and shape "
                                        + Arrays.toString( shape ),
                                        e );
        }

        if ( rawValues.length != countOfRawValuesToRead )
        {
            throw new PreReadException(
                    "Expected to read exactly " + countOfRawValuesToRead
                    + " values from variable "
                    + variableName
                    + " instead got "
                    + rawValues.length );
        }

        // Write the values to the result array. Skip past unrequested values.
        int lastRawIndex = -1;
        for ( int i = 0; i < indices.length; i++ )
        {
            if ( i == 0 )
            {
                result[0] = rawValues[0];
                lastRawIndex = 0;
            }
            else
            {
                // The distance between the indices passed implies the spot
                // in the (superset) of rawValues array returned.
                int rawIndexHop = indices[i] - indices[i - 1];
                int currentRawIndex = lastRawIndex + rawIndexHop;
                result[i] = rawValues[currentRawIndex];
                lastRawIndex = currentRawIndex;
            }
        }

        LOGGER.debug( "Asked variable {} for range {} through {} to get values at indices {}, got raw count {}, "
                      + "distilled to {}",
                      variableName,
                      minIndex,
                      maxIndex,
                      indices,
                      rawValues.length,
                      result );
        return result;
    }


    /**
     * Given an integer raw (packed) value, use the given attributes to unpack.
     * @param rawValue The raw (packed) int value.
     * @param attributes The attributes associated with the variable.
     * @return The value unpacked into a double.
     */
    private static double unpack( int rawValue, VariableAttributes attributes )
    {
        if ( attributes.has32BitPacking() )
        {
            return rawValue * attributes.multiplier32()
                   + attributes.offsetToAdd32();
        }

        return rawValue * attributes.multiplier64()
               + attributes.offsetToAdd64();
    }


    /**
     * Helper to read minutes from epoch into an Instant
     * @param netcdfFile the (open) netCDF file to read from
     * @param variableName the name of the variable to read
     *                     assumes cardinality 1
     *                     assumes the value is an int
     *                     assumes the value is minutes since unix epoch
     * @return the Instant representation of the value
     */

    private static Instant readMinutesFromEpoch( NetcdfFile netcdfFile,
                                                 String variableName )
    {
        Variable ncVariable = netcdfFile.findVariable( variableName );
        assert ncVariable != null;

        try
        {
            Array allValidDateTimes = ncVariable.read();
            int minutesSinceEpoch = allValidDateTimes.getInt( 0 );
            Duration durationSinceEpoch = Duration.ofMinutes( minutesSinceEpoch );
            return Instant.ofEpochSecond( durationSinceEpoch.toSeconds() );
        }
        catch ( IOException ioe )
        {
            throw new PreReadException( "Failed to read Instant for variable "
                                        + ncVariable
                                        + " from netCDF file "
                                        + netcdfFile );
        }
    }

    /**
     * Attributes that have been actually read from a netCDF Variable.
     * The purpose of this class is to help avoid re-reading the same metadata.
     * @param has32BitPacking  True means use float multiplier and offset, false means double
     */

    private record VariableAttributes( int missingValue, int fillValue, boolean has32BitPacking, float multiplier32,
                                       float offsetToAdd32, double multiplier64, double offsetToAdd64 )
    {
    }

    /**
     * Task that performs open on given URI.
     */

    private record NWMResourceOpener( URI uri ) implements Callable<NetcdfFile>
    {
        private NWMResourceOpener
        {
            Objects.requireNonNull( uri );
        }

        @Override
        public NetcdfFile call() throws IOException
        {
            LOGGER.debug( "Opening NetCDF resource, '{}'", this.uri );
            NetcdfFile file = NetcdfFiles.open( this.uri.toString() );
            LOGGER.debug( "Opened NetCDF resource, '{}'", this.uri );
            return file;
        }
    }

    private static final class EventForNWMFeature<T>
    {
        private static final int NO_MEMBER = Integer.MIN_VALUE;
        private final long featureId;
        private final Event<T> event;
        private final int ensembleMemberNumber;

        EventForNWMFeature( long featureId, Event<T> event )
        {
            this( featureId, event, NO_MEMBER );
        }

        EventForNWMFeature( long featureId, Event<T> event, int ensembleMemberNumber )
        {
            Objects.requireNonNull( event );
            this.featureId = featureId;
            this.event = event;
            this.ensembleMemberNumber = ensembleMemberNumber;
        }

        long getFeatureId()
        {
            return featureId;
        }

        Event<T> getEvent()
        {
            return this.event;
        }

        int getEnsembleMemberNumber()
        {
            if ( this.ensembleMemberNumber == NO_MEMBER )
            {
                throw new IllegalStateException( "No member was set." );
            }

            return this.ensembleMemberNumber;
        }
    }

    /**
     * @param data the data
     * @param featuresNotFound the features not found
     */
    private record NWMDoubleReadOutcome( List<EventForNWMFeature<Double>> data,
                                         Set<Long> featuresNotFound )
    {
    }

    /**
     * Task that reads a single event from given NWM profile, variables, etc.
     */

    private static final class NWMDoubleReader implements Callable<NWMDoubleReadOutcome>
    {
        private static final int NOT_FOUND = Integer.MIN_VALUE;
        private final NwmProfile profile;
        private final NetcdfFile netcdfFile;
        private final long[] featureIds;
        private final String variableName;
        private final Instant originalReferenceDatetime;
        private final NWMFeatureCache featureCache;
        private final boolean isEnsemble;
        private final Set<Long> featuresNotFound;

        NWMDoubleReader( NwmProfile profile,
                         NetcdfFile netcdfFile,
                         long[] featureIds,
                         String variableName,
                         Instant originalReferenceDatetime,
                         NWMFeatureCache featureCache,
                         boolean isEnsemble )
        {
            Objects.requireNonNull( profile );
            Objects.requireNonNull( netcdfFile );
            Objects.requireNonNull( variableName );
            Objects.requireNonNull( originalReferenceDatetime );
            Objects.requireNonNull( featureCache );
            this.profile = profile;
            this.netcdfFile = netcdfFile;
            this.featureIds = featureIds;
            this.variableName = variableName;
            this.originalReferenceDatetime = originalReferenceDatetime;
            this.featureCache = featureCache;
            this.isEnsemble = isEnsemble;
            this.featuresNotFound = new HashSet<>( 1 );
        }

        @Override
        public NWMDoubleReadOutcome call()
        {
            return this.readDoubles( this.profile,
                                     this.netcdfFile,
                                     this.featureIds,
                                     this.variableName,
                                     this.originalReferenceDatetime,
                                     this.featureCache,
                                     this.isEnsemble );
        }

        private NWMDoubleReadOutcome readDoubles( NwmProfile profile,
                                                  NetcdfFile netcdfFile,
                                                  long[] featureIds,
                                                  String variableName,
                                                  Instant originalReferenceDatetime,
                                                  NWMFeatureCache featureCache,
                                                  boolean isEnsemble )
        {
            // Get the valid datetime
            Instant validDatetime = NwmTimeSeries.readValidDatetime( profile,
                                                                     netcdfFile );

            // Get the reference datetime
            Instant ncReferenceDatetime = NwmTimeSeries.readReferenceDatetime( profile,
                                                                               netcdfFile );

            // Validate: this referenceDatetime should match what was set originally,
            // except for analysis/assim data.
            if ( !profile.getNwmConfiguration()
                         .toLowerCase()
                         .contains( "analysis" )
                 && !ncReferenceDatetime.equals( originalReferenceDatetime ) )
            {
                throw new PreReadException( "The reference datetime "
                                            + ncReferenceDatetime
                                            + " from netCDF resource "
                                            + netcdfFile.getLocation()
                                            + " does not match expected value "
                                            + originalReferenceDatetime );
            }

            int ncEnsembleMember = NOT_FOUND;

            if ( isEnsemble )
            {
                ncEnsembleMember = this.readEnsembleNumber( profile, netcdfFile );
            }

            // Discover the minimum and maximum indexes requested while getting
            // them from the featureCache in order to know the nc range to read.
            // Initialize to extreme values to detect when nothing was found
            // and to have some assurance that "less than" and "greater than"
            // will work from the start of the loop.
            FeatureDetails featureDetails = this.getFeatureDetails( profile, netcdfFile, featureIds, featureCache );
            List<FeatureIdWithItsIndex> features = featureDetails.features();
            int minIndex = featureDetails.minIndex();
            int maxIndex = featureDetails.maxIndex();
            this.featuresNotFound.addAll( featureDetails.featuresNotFound() );

            if ( minIndex == Integer.MAX_VALUE
                 || maxIndex == Integer.MIN_VALUE )
            {
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "No features found, minIndex==MAX? {}, maxIndex==MIN? {}, features requested: {}",
                                  minIndex == Integer.MAX_VALUE,
                                  maxIndex == Integer.MIN_VALUE,
                                  featureIds );
                }

                return new NWMDoubleReadOutcome( Collections.emptyList(),
                                                 this.featuresNotFound );
            }

            // Filtered for non-existent feature ids:
            int[] indicesOfFeatures = features.stream()
                                              .filter( FeatureIdWithItsIndex::found )
                                              .mapToInt( FeatureIdWithItsIndex::index )
                                              .toArray();
            long[] companionFeatures = features.stream()
                                               .filter( FeatureIdWithItsIndex::found )
                                               .mapToLong( FeatureIdWithItsIndex::featureId )
                                               .toArray();

            Variable variableVariable = netcdfFile.findVariable( variableName );
            int[] rawVariableValues = this.getRawVariableValues( variableVariable,
                                                                 indicesOfFeatures,
                                                                 companionFeatures,
                                                                 minIndex,
                                                                 maxIndex,
                                                                 netcdfFile );

            VariableAttributes attributes = NwmTimeSeries.readVariableAttributes( variableVariable );

            List<EventForNWMFeature<Double>> list = new ArrayList<>( rawVariableValues.length );

            int missingValue = attributes.missingValue();
            int fillValue = attributes.fillValue();

            for ( int i = 0; i < rawVariableValues.length; i++ )
            {
                double variableValue;

                if ( rawVariableValues[i] == missingValue
                     || rawVariableValues[i] == fillValue )
                {
                    LOGGER.debug( "Found missing value {} (one of {}, {}) at index {} for feature {} in variable {} of "
                                  + "netCDF {}",
                                  rawVariableValues[i],
                                  missingValue,
                                  fillValue,
                                  i,
                                  companionFeatures[i],
                                  variableName,
                                  netcdfFile.getLocation() );
                    variableValue = MissingValues.DOUBLE;
                }
                else
                {
                    // Unpack.
                    variableValue = NwmTimeSeries.unpack( rawVariableValues[i],
                                                          attributes );
                }

                Event<Double> event = DoubleEvent.of( validDatetime, variableValue );

                if ( isEnsemble )
                {
                    EventForNWMFeature<Double> eventWithFeatureId =
                            new EventForNWMFeature<>( companionFeatures[i],
                                                      event,
                                                      ncEnsembleMember );
                    list.add( eventWithFeatureId );
                }
                else
                {
                    EventForNWMFeature<Double> eventWithFeatureId =
                            new EventForNWMFeature<>( companionFeatures[i],
                                                      event );
                    list.add( eventWithFeatureId );
                }
            }

            LOGGER.trace( "Read raw values {} for variable {} at {} returning as values {} from {}",
                          rawVariableValues,
                          variableName,
                          validDatetime,
                          list,
                          netcdfFile );

            return new NWMDoubleReadOutcome( Collections.unmodifiableList( list ),
                                             Collections.unmodifiableSet( this.featuresNotFound ) );
        }

        /**
         * Reads the raw variable values from the inputs.
         *
         * @param variableVariable the variable value
         * @param indicesOfFeatures the feature indexes
         * @param companionFeatures the companion features
         * @param minIndex the minimum feature index
         * @param maxIndex the maximum feature index
         * @param netcdfFile the NetCDF file
         * @return the raw variable values
         */

        private int[] getRawVariableValues( Variable variableVariable,
                                            int[] indicesOfFeatures,
                                            long[] companionFeatures,
                                            int minIndex,
                                            int maxIndex,
                                            NetcdfFile netcdfFile )
        {
            int[] rawVariableValues;


            // Preserve the context of which netCDF blob is read with try/catch.
            // (The readRawInts method does not need the netCDF blob.)
            try
            {
                rawVariableValues = NwmTimeSeries.readRawInts( variableVariable,
                                                               indicesOfFeatures,
                                                               minIndex,
                                                               maxIndex );
                LOGGER.debug( "Read integer values {} corresponding to feature ids {} at indices {} from {}",
                              rawVariableValues,
                              companionFeatures,
                              indicesOfFeatures,
                              netcdfFile.getLocation() );

                return rawVariableValues;
            }
            catch ( PreReadException pie )
            {
                throw new PreReadException( "While reading netCDF data at "
                                            + netcdfFile.getLocation(),
                                            pie );
            }
        }

        /**
         * Returns the feature details.
         * @param profile the profile
         * @param netcdfFile the NetCDF file
         * @param featureIds the feature IDs
         * @param featureCache the feature cache
         * @return the feature details
         */

        private FeatureDetails getFeatureDetails( NwmProfile profile,
                                                  NetcdfFile netcdfFile,
                                                  long[] featureIds,
                                                  NWMFeatureCache featureCache )
        {
            // Discover the minimum and maximum indexes requested while getting
            // them from the featureCache in order to know the nc range to read.
            // Initialize to extreme values to detect when nothing was found
            // and to have some assurance that "less than" and "greater than"
            // will work from the start of the loop.
            int minIndex = Integer.MAX_VALUE;
            int maxIndex = Integer.MIN_VALUE;

            List<FeatureIdWithItsIndex> features = new ArrayList<>( featureIds.length );
            Set<Long> notFound = new HashSet<>();
            for ( long featureId : featureIds )
            {
                FeatureIdWithItsIndex feature;
                int indexOfFeature = featureCache.findFeatureIndex( profile,
                                                                    netcdfFile,
                                                                    featureId );
                feature = new FeatureIdWithItsIndex( featureId, indexOfFeature );
                features.add( feature );

                if ( !feature.found() )
                {
                    notFound.add( feature.featureId() );
                    continue;
                }

                if ( feature.index() < minIndex )
                {
                    minIndex = feature.index();
                }

                if ( feature.index() > maxIndex )
                {
                    maxIndex = feature.index();
                }
            }

            return new FeatureDetails( minIndex, maxIndex, features, notFound );
        }

        /**
         * <p>Reads and validates the ensemble number from an NWM netCDF resource.
         *
         * <p>Assumes that NWM netCDF resources only have one ensemble number in
         * the global attributes.
         *
         * @param profile The profile describing the netCDF dataset.
         * @param netcdfFile The individual netCDF resource to read.
         * @return The member number from the global attributes.
         * @throws PreReadException When ensemble member attribute is missing.
         * @throws PreReadException When ensemble number exceeds count in profile.
         * @throws PreReadException When ensemble number is less than 1.
         */
        private int readEnsembleNumber( NwmProfile profile,
                                        NetcdfFile netcdfFile )
        {

            int memberCount = profile.getMemberCount();

            // Get the ensemble_member_number
            String memberNumberAttributeName = profile.getMemberAttribute();
            List<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();

            int ncEnsembleNumber = NOT_FOUND;

            for ( Attribute globalAttribute : globalAttributes )
            {
                if ( globalAttribute.getName()
                                    .equals( memberNumberAttributeName ) )
                {
                    ncEnsembleNumber = Objects.requireNonNull( globalAttribute.getNumericValue() )
                                              .intValue();
                    break;
                }
            }

            if ( ncEnsembleNumber == NOT_FOUND )
            {
                throw new PreReadException( "Could not find ensemble member attribute "
                                            + memberNumberAttributeName
                                            +
                                            " in netCDF file "
                                            + netcdfFile );
            }

            if ( ncEnsembleNumber > memberCount )
            {
                throw new PreReadException( "Ensemble number "
                                            + ncEnsembleNumber
                                            + " unexpectedly exceeds member count "
                                            + memberCount );
            }

            if ( ncEnsembleNumber < 1 )
            {
                throw new PreReadException( "Ensemble number "
                                            + ncEnsembleNumber
                                            + " is unexpectedly less than 1." );
            }

            return ncEnsembleNumber;
        }

    }


    /**
     * Cache that stores a single copy of a feature array for a Set of netCDFs.
     * But also compares the feature array once for each netCDF resource to
     * validate that it is in fact an exact copy.
     */

    private static final class NWMFeatureCache
    {
        /**
         * <p>The contents of the feature variable for a given netCDF resource.
         *
         * <p>This is a performance optimization (avoid re-reading the features).
         * GuardedBy featureCacheGuard
         *
         * <p>This is kept in int[] form in order to validate new netCDF resources.
         */
        private long[] originalFeatures;

        /**
         * <p>Whether the features stored is sorted. Allows binary search if true.
         *
         * <p>Only one Thread should write this value after construction, the rest
         * read it.
         *
         * <p>This is a performance optimization (avoid linear search of features).
         *
         * <p>Not certain that this needs to be volatile, but just in case...
         *
         * <p>GuardedBy {@link #featureCacheGuard}
         */
        private volatile boolean isSorted = false;

        /**
         * <p>First Thread to set to true wins
         * and gets to write to the featureCache. Everyone else reads.
         */
        private final AtomicBoolean featureCacheRaceResolver = new AtomicBoolean( false );

        /**
         * <p>Guards featureCache variable.
         */
        private final CountDownLatch featureCacheGuard = new CountDownLatch( 1 );

        /**
         * Tracks which netCDF resoruces have had their features validated.
         */
        private final Map<NetcdfFile, AtomicBoolean> validatedFeatures;

        NWMFeatureCache( Set<NetcdfFile> netcdfUris )
        {
            this.validatedFeatures = new HashMap<>( netcdfUris.size() );

            for ( NetcdfFile netcdfFile : netcdfUris )
            {
                AtomicBoolean notValidated = new AtomicBoolean( false );
                this.validatedFeatures.put( netcdfFile, notValidated );
            }
        }

        /**
         * Find the given featureId index within the given netCDF blob using the
         * given NwmProfile and this feature cache.
         * @param profile The profile to use (has name of feature variable).
         * @param netcdfFile The netCDF blob to search.
         * @param featureId The NWM feature id to search for.
         * @return The index of the featureID within the feature variable, or
         * a negative integer when not found.
         */

        private int findFeatureIndex( NwmProfile profile,
                                      NetcdfFile netcdfFile,
                                      long featureId )
        {
            long[] features = this.readFeaturesAndCacheOrGetFeaturesFromCache( profile,
                                                                               netcdfFile );

            if ( this.isSorted )
            {
                return Arrays.binarySearch( features, featureId );
            }
            else
            {
                LOGGER.debug( "Doing linear search of NWM features cache." );

                for ( int i = 0; i < features.length; i++ )
                {
                    if ( features[i] == featureId )
                    {
                        return i;
                    }
                }

                // Could not find, return a negative number.
                return -1;
            }
        }

        /**
         * Wait for the cache to be set. This method will interrupt this thread
         * if the process of waiting for the cache is, itself, interrupted.
         * @param netcdfFileName Name of netCDF being read.
         * @throws PreReadException when timed out waiting on cache.
         */
        private void waitForCache( String netcdfFileName )
        {
            try
            {
                Duration timeout = Duration.ofMinutes( 10 );
                boolean timedOut = !this.featureCacheGuard.await( timeout.toSeconds(),
                                                                  TimeUnit.SECONDS );
                if ( timedOut )
                {
                    throw new PreReadException(
                            "While reading "
                            + netcdfFileName,
                            new TimeoutException(
                                    "Timed out waiting for feature cache after "
                                    + timeout ) );
                }
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while waiting for feature cache to be written", ie );
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Read features and cache in instance or read from the cache. Allows us to
         * avoid multiple reads of same data.
         * @param profile the profile
         * @param netcdfFile the netCDF file
         * @return the raw 1D integer array of features, in original positions.
         * Performs safe publication of an internal map, assumes internal
         * caller, assumes that internal caller will not publish the map,
         * assumes internal caller will only read from the map.
         * @throws PreReadException From the waitForCache method, will be
         * thrown if the wait times out.
         */

        private long[] readFeaturesAndCacheOrGetFeaturesFromCache( NwmProfile profile,
                                                                   NetcdfFile netcdfFile )
        {
            String netcdfFileName = netcdfFile.getLocation();

            // Check to see if this netcdf file has features already read
            // If its already been read, then wait for the cache to be set
            // and return what was cached.
            AtomicBoolean validated = this.validatedFeatures.get( netcdfFile );
            if ( validated.get() )
            {
                LOGGER.trace( "Already read netCDF resource {}, returning cached.",
                              netcdfFileName );
                waitForCache( netcdfFileName );
                return this.originalFeatures;
            }
            else
            {
                LOGGER.trace( "Did not yet read netCDF resource {}",
                              netcdfFileName );
            }

            String featureVariableName = profile.getFeatureVariable();
            Variable featureVariable = netcdfFile.findVariable( featureVariableName );
            assert featureVariable != null;

            // Must find the location of the variable.
            // Might be nice to assume these are sorted to do a binary search,
            // but I think it is an unsafe assumption that the values are sorted
            // and we are looking for the index of the feature id to use for
            // getting a value from the actual variable needed.
            try
            {
                // Discover if this Thread is responsible for setting featureCache
                boolean previouslySet = this.featureCacheRaceResolver.getAndSet( true );

                //If this Thread is not setting the cache, then wait until the cache
                //is set.
                if ( previouslySet )
                {
                    waitForCache( netcdfFileName );
                }

                //Read the features from the netCDF file and clone them to ensure that
                //we are referring to a fresh copy, and not an internal netCDF copy.
                LOGGER.debug( "Reading features from {}", netcdfFileName );
                long[] features = ( long[] ) featureVariable.read()
                                                            .get1DJavaArray( DataType.LONG );
                if ( features == null )
                {
                    throw new IllegalStateException( "netCDF library returned null array when looking for NWM features." );
                }
                features = features.clone();

                //Set the cache if this Thread is responsible for it.
                if ( !previouslySet )
                {
                    LOGGER.debug( "About to set the features cache using {}",
                                  netcdfFileName );

                    // In charge of setting the feature cache, do so.
                    this.originalFeatures = features.clone();

                    // Sort the local features, compare to see if identical and
                    // set isSorted.
                    Arrays.sort( features );
                    if ( Arrays.equals( features, this.originalFeatures ) )
                    {
                        this.isSorted = true;
                    }

                    // Tell waiting Threads that featureCache hath been written.
                    this.featureCacheGuard.countDown();
                    LOGGER.debug( "Finished setting the features cache using {}",
                                  netcdfFileName );
                }
                //Otherwise, just check the read features against the cached
                //features to ensure they are identical.
                else
                {
                    LOGGER.debug( "About to read the features cache to compare {}",
                                  netcdfFileName );

                    // Compare the existing features with those just read, throw an
                    // exception if they differ (by content).
                    if ( !Arrays.equals( features, this.originalFeatures ) )
                    {
                        throw new PreReadException( "Non-homogeneous NWM data found. The features from "
                                                    + netcdfFile.getLocation()
                                                    + " do not match those found in a previously read "
                                                    + "netCDF resource in the same NWM timeseries." );
                    }
                    LOGGER.debug( "Finished comparing {} to the features cache",
                                  netcdfFileName );
                }

                validated.set( true );

                // Assumes caller will only perform reads, will not publish.
                return this.originalFeatures;
            }
            catch ( IOException ioe )
            {
                throw new PreReadException( "Failed to read features from "
                                            + netcdfFileName,
                                            ioe );
            }
        }
    }

    /**
     * Returns a URI from a base URI and resource name separated with a character that depends on the URI scheme. For
     * the cdsm3 scheme, uses a '?' character, otherwise a '/' character. The cdms3 scheme is used by the Unidata
     * NetCDF library to read data from a cloud bucket via the S3 or GCS APIs, which requires a resource key.  The
     * resource key is separated with a '?' character.
     *
     * @param baseUri the base uri
     * @param resourceName the resource name
     * @return the separator character
     */

    private static URI getUriForScheme( URI baseUri, String resourceName )
    {
        if ( "cdms3".equalsIgnoreCase( baseUri.getScheme() ) )
        {
            String resourceUri = baseUri.toString()
                                        .replaceAll( "/$", "?" + resourceName );

            return URI.create( resourceUri );
        }

        return baseUri.resolve( resourceName );
    }

    /**
     * @param featureId the feature identifier
     * @param index the index
     */
    private record FeatureIdWithItsIndex( long featureId, int index )
    {
        boolean found()
        {
            return this.index >= 0;
        }
    }

    /**
     * Feature details.
     *
     * @param minIndex the minimum index
     * @param maxIndex the maximum index
     * @param features the features found
     * @param featuresNotFound the features not found
     */
    private record FeatureDetails( int minIndex,
                                   int maxIndex,
                                   List<FeatureIdWithItsIndex> features,
                                   Set<Long> featuresNotFound ) {}

}