package wres.io.reading.nwm;

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
import java.util.StringJoiner;
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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.PreIngestException;
import wres.system.SystemSettings;

/**
 * Goal: a variable/feature/timeseries combination in a Vector NWM dataset is
 * considered a source.
 *
 * Only the variable and NWM feature selected for an evaluation will be ingested
 * from a vector NWM dataset.
 *
 * All the NWM netCDF blobs for a given timeseries will first be found and
 * opened prior to attempting to identify the timeseries, prior to attempting
 * to ingest any rows of timeseries data.
 *
 * This class opens a set of NWM netCDF blobs as a timeseries, based on a profile.
 *
 * This class intentionally only deals with NWM feature ids (not WRES ids).
 */

class NWMTimeSeries implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NWMTimeSeries.class );
    private static final DateTimeFormatter NWM_DATE_FORMATTER = DateTimeFormatter.ofPattern( "yyyyMMdd" );
    private static final DateTimeFormatter NWM_HOUR_FORMATTER = DateTimeFormatter.ofPattern( "HH" );

    private static final int CONCURRENT_READS = 6;

    private final NWMProfile profile;

    /** The reference datetime of this NWM Forecast */
    private final Instant referenceDatetime;

    /** The base URI from where to find the members of this forecast */
    private final URI baseUri;

    /**
     * The netCDF resources managed by this instance, opened on construction
     * and closed on close().
     */
    private final Set<NetcdfFile> netcdfFiles;

    /**
     * The cache holding NWM feature ids for this whole set of NWM resources
     */
    private final NWMFeatureCache featureCache;

    /**
     * To parallelize requests for data from netCDF resources.
     */
    private final ThreadPoolExecutor readExecutor;

    /**
     * List of features requested that were not found in this NWM TimeSeries
     */
    private final Set<Integer> featuresNotFound;

    /**
     *
     * @param profile
     * @param referenceDatetime
     * @param baseUri
     * @throws NullPointerException When any argument is null.
     * @throws PreIngestException When any netCDF blob could not be opened.
     * @throws IllegalArgumentException When baseUri is not absolute.
     */

    NWMTimeSeries( SystemSettings systemSettings,
                   NWMProfile profile,
                   Instant referenceDatetime,
                   URI baseUri )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( profile );
        Objects.requireNonNull( referenceDatetime );
        Objects.requireNonNull( baseUri );
        this.profile = profile;
        this.referenceDatetime = referenceDatetime;

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
        Set<URI> netcdfUris = NWMTimeSeries.getNetcdfUris( profile,
                                                           referenceDatetime,
                                                           this.baseUri );
        this.netcdfFiles = new HashSet<>( netcdfUris.size() );
        LOGGER.debug( "Attempting to open NWM TimeSeries with reference datetime {} and profile {} from baseUri {}.",
                      referenceDatetime, profile, this.baseUri );

        ThreadFactory nwmReaderThreadFactory = new BasicThreadFactory.Builder()
                .namingPattern( "NWMTimeSeries Reader %d" )
                .build();

        // See comments in WebSource class regarding the setup of the executor,
        // queue, and latch.
        BlockingQueue<Runnable> nwmReaderQueue =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        this.readExecutor = new ThreadPoolExecutor( CONCURRENT_READS,
                                                    CONCURRENT_READS,
                                                    systemSettings.poolObjectLifespan(),
                                                    TimeUnit.MILLISECONDS,
                                                    nwmReaderQueue,
                                                    nwmReaderThreadFactory );
        this.readExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
        BlockingQueue<Future<NetcdfFile>> openBlobQueue =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        CountDownLatch startGettingResults =
                new CountDownLatch( CONCURRENT_READS );

        this.featuresNotFound = new HashSet<>( 1 );

        // Open all the relevant files during construction, or fail.
        for ( URI netcdfUri : netcdfUris )
        {
            NWMResourceOpener opener = new NWMResourceOpener( netcdfUri );
            Future<NetcdfFile> futureBlob = this.readExecutor.submit( opener );
            openBlobQueue.add( futureBlob );
            startGettingResults.countDown();

            if ( startGettingResults.getCount() <= 0 )
            {
                try
                {
                    NetcdfFile netcdfFile = openBlobQueue.take()
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
                    Throwable cause = ee.getCause();

                    if ( Objects.nonNull( cause )
                         && cause instanceof FileNotFoundException )
                    {
                        LOGGER.warn( "Skipping resource not found: {}",
                                     cause.getMessage() );
                    }
                    else
                    {
                        this.close();
                        throw new PreIngestException( "Failed to open netCDF resource.",
                                                      ee );
                    }
                }
            }
        }

        // Finish getting the remainder of netCDF resources being opened.
        for ( Future<NetcdfFile> opening : openBlobQueue )
        {
            try
            {
                NetcdfFile netcdfFile = opening.get();
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
                Throwable cause = ee.getCause();

                if ( Objects.nonNull( cause )
                     && cause instanceof FileNotFoundException )
                {
                    LOGGER.warn( "Skipping resource not found: {}",
                                 cause.getMessage() );
                }
                else
                {
                    this.close();
                    throw new PreIngestException( "Failed to open netCDF resource.",
                                                  ee );
                }
            }
        }

        this.featureCache = new NWMFeatureCache( this.netcdfFiles );

        if ( netcdfUris.size() == this.netcdfFiles.size() )
        {
            LOGGER.debug( "Successfully opened NWM TimeSeries with reference datetime {} and profile {} from baseUri {}.",
                          referenceDatetime, profile, this.baseUri );
        }
        else if ( this.netcdfFiles.size() == 0 )
        {
            LOGGER.warn( "Skipping NWM TimeSeries (not found) with reference datetime {} and profile {} from baseUri {}.",
                         referenceDatetime, profile, this.baseUri );
        }
        else
        {
            LOGGER.warn( "Found a partial NWM TimeSeries with reference datetime {} and profile {} from baseUri {}.",
                         referenceDatetime, profile, this.baseUri );
        }
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

    static Set<URI> getNetcdfUris( NWMProfile profile,
                                   Instant referenceDatetime,
                                   URI baseUri )
    {
        LOGGER.debug( "Called getNetcdfUris with {}, {}, {}", profile,
                      referenceDatetime, baseUri );
        Set<URI> uris = new HashSet<>();
        final String NWM_DOT = "nwm.";

        // Formatter cannot handle Instant
        OffsetDateTime referenceOffsetDateTime = OffsetDateTime.ofInstant( referenceDatetime,
                                                                           ZoneId.of( "UTC" ) );
        String nwmDatePath = NWM_DOT
                             + NWM_DATE_FORMATTER.format( referenceOffsetDateTime );

        for ( short i = 1; i <= profile.getMemberCount(); i++ )
        {
            URI uriWithDate = baseUri.resolve( nwmDatePath + "/" );

            String directoryName = profile.getNwmSubdirectoryPrefix();

            if ( profile.isEnsembleLike() )
            {
                directoryName += "_mem" + i ;
            }

            URI uriWithDirectory = uriWithDate.resolve( directoryName + "/" );

            for ( short j = 1; j <= profile.getBlobCount(); j++ )
            {
                String ncFilePartOne = NWM_DOT + "t"
                                       + NWM_HOUR_FORMATTER.format( referenceOffsetDateTime )
                                       + "z." + profile.getNwmConfiguration()
                                       + "." + profile.getNwmOutputType();

                // Ensemble number appended if greater than one member present.
                if ( profile.isEnsembleLike() )
                {
                    ncFilePartOne += "_" + i;
                }

                String ncFilePartTwo = "." + profile.getTimeLabel();

                long hours = profile.getDurationBetweenValidDatetimes()
                                    .toHours()
                             * j;

                if ( profile.getTimeLabel()
                            .equals( NWMProfile.TimeLabel.f ) )
                {
                    String forecastLabel = String.format( "%03d", hours );
                    ncFilePartTwo += forecastLabel;
                }
                else if ( profile.getTimeLabel()
                                 .equals( NWMProfile.TimeLabel.tm ))
                {
                    // Analysis files go back in valid datetime as j increases.
                    String analysisLabel = String.format( "%02d", j - 1 );
                    ncFilePartTwo += analysisLabel;
                }

                String ncFilePartThree = "." + profile .getNwmLocationLabel()
                                         + ".nc";
                String ncFile = ncFilePartOne + ncFilePartTwo + ncFilePartThree;
                LOGGER.trace( "Built a netCDF filename: {}", ncFile );

                URI fullUri = uriWithDirectory.resolve( ncFile );
                uris.add( fullUri );
            }
        }

        LOGGER.debug( "Returning these netCDF URIs: {}", uris );
        return Collections.unmodifiableSet( uris );
    }

    NWMProfile getProfile()
    {
        return this.profile;
    }

    Instant getReferenceDatetime()
    {
        return this.referenceDatetime;
    }

    URI getBaseUri()
    {
        return this.baseUri;
    }

    private Set<NetcdfFile> getNetcdfFiles()
    {
        return this.netcdfFiles;
    }

    private String getNetcdfResourceNames()
    {
        StringJoiner joiner = new StringJoiner( ", ", "( ", " )" );
        for ( NetcdfFile netcdfFile : this.getNetcdfFiles() )
        {
            String netcdfResourceName = netcdfFile.getLocation();
            joiner.add( netcdfResourceName );
        }

        return joiner.toString();
    }

    int countOfNetcdfFiles()
    {
        return this.getNetcdfFiles().size();
    }


    Map<Integer,TimeSeries<?>> readEnsembleTimeSerieses( int[] featureIds,
                                                         String variableName,
                                                         String unitName )
            throws InterruptedException, ExecutionException
    {

        int memberCount = this.getProfile().getMemberCount();
        int validDatetimeCount = this.getProfile().getBlobCount();

        // Map of nwm feature id to a map of each timestep with member values.
        Map<Integer,Map<Instant,double[]>> ensembleValues = new HashMap<>( featureIds.length );

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
                List<EventForNWMFeature<Double>> read = outcome.getData();
                this.featuresNotFound.addAll( outcome.getFeaturesNotFound() );
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
            List<EventForNWMFeature<Double>> read = outcome.getData();
            this.featuresNotFound.addAll( outcome.getFeaturesNotFound() );
            this.putEnsembleDataInMap( read,
                                       ensembleValues,
                                       memberCount,
                                       validDatetimeCount );
        }

        for ( Map.Entry<Integer,Map<Instant,double[]>> oneEnsemble : ensembleValues.entrySet() )
        {
            Map<Instant,double[]> map = oneEnsemble.getValue();
            if ( map.size() != validDatetimeCount )
            {
                throw new PreIngestException( "Expected "
                                              + validDatetimeCount
                                              + " different valid datetimes but found "
                                              + map.size()
                                              + " in netCDF resources "
                                              + this.getNetcdfResourceNames()
                                              + " for NWM feature id "
                                              + oneEnsemble.getKey() );
            }
        }

        Map<Integer,TimeSeries<?>> byFeatureId = new HashMap<>( featureIds.length
                                                                - this.featuresNotFound.size() );

        // For each feature, create a TimeSeries.
        for ( Map.Entry<Integer,Map<Instant,double[]>> entriesForOne : ensembleValues.entrySet() )
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

            FeatureKey feature = new FeatureKey( entriesForOne.getKey()
                                                              .toString(),
                                                 null,
                                                 null,
                                                 null );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, this.getReferenceDatetime() ),
                                                                 null,
                                                                 variableName,
                                                                 feature,
                                                                 unitName );
            // Create the TimeSeries for the current Feature
            TimeSeries<?> timeSeries = TimeSeries.of( metadata,
                                                      sortedEvents );

            // Store this TimeSeries in the collection to be returned.
            byFeatureId.put( entriesForOne.getKey(), timeSeries );
        }

        return Collections.unmodifiableMap( byFeatureId );
    }


    /**
     * Read data into intermediate format more convenient for wres.datamodel.
     * @param events Reads this data to put into ensembleValues.
     * @param ensembleValues MUTATES this data using data found from events.
     * @param memberCount The count of ensemble members.
     * @param validDatetimeCount The count of validDatetimes.
     */
    private void putEnsembleDataInMap( List<EventForNWMFeature<Double>> events,
                                       Map<Integer,Map<Instant,double[]>> ensembleValues,
                                       int memberCount,
                                       int validDatetimeCount )
    {

        for ( EventForNWMFeature<Double> event : events )
        {
            Instant eventDatetime = event.getEvent()
                                         .getTime();
            Integer featureId = event.getFeatureId();
            Map<Instant,double[]> fullEnsemble = ensembleValues.get( featureId );

            if ( Objects.isNull( fullEnsemble )  )
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

    /**
     * Read the first value for a given variable name attribute from the netCDF
     * files.
     * @param variableName The NWM variable name.
     * @param attributeName The attribute associated with the variable.
     * @return The String representation of the value of attribute of variable.
     */

    String readAttributeAsString( String variableName, String attributeName )
    {
        if ( !this.getNetcdfFiles().isEmpty() )
        {
            // Use the very first netcdf file, assume homogeneity.
            NetcdfFile netcdfFile = this.getNetcdfFiles()
                                        .iterator()
                                        .next();
            Variable variableVariable = netcdfFile.findVariable( variableName );

            if ( variableVariable == null )
            {
                throw new IllegalArgumentException( "No variable '"
                                                    + variableName
                                                    + "' found in netCDF data "
                                                    + netcdfFile );
            }

            return readAttributeAsString( variableVariable, attributeName );
        }
        else
        {
            throw new IllegalStateException( "No netCDF data available." );
        }
    }


    private static DataType getAttributeType( Variable ncVariable,
                                              String attributeName )
    {
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
            {
                return attribute.getDataType();
            }
        }

        throw new IllegalStateException( "No '" + attributeName
                                         + "' attribute found for variable '"
                                         + ncVariable + " in netCDF data." );
    }

    /**
     * @param ncVariable The NWM variable.
     * @param attributeName The attribute associated with the variable.
     * @return The String representation of the value of attribute of variable.
     */

    private static String readAttributeAsString( Variable ncVariable,
                                                 String attributeName )
    {
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
            {
                return attribute.getStringValue();
            }
        }

        throw new IllegalStateException( "No '" + attributeName
                                         + "' attribute found for variable '"
                                         + ncVariable + " in netCDF data." );
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
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
            {
                DataType type = attribute.getDataType();

                if ( type.equals( DataType.DOUBLE ) )
                {
                    return attribute.getNumericValue()
                                    .doubleValue();
                }
                else
                {
                    throw new CastMayCauseBadConversionException(
                            "Unable to convert attribute '"
                            + attributeName
                            + "' to double because it is type "
                            + type );
                }
            }
        }

        throw new IllegalArgumentException( "No '" + attributeName
                                            + "' attribute found for variable '"
                                            + ncVariable + " in netCDF data." );
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
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
            {
                DataType type = attribute.getDataType();

                if ( type.equals( DataType.FLOAT ) )
                {
                    return attribute.getNumericValue()
                                    .floatValue();
                }
                else
                {
                    throw new CastMayCauseBadConversionException(
                            "Unable to convert attribute '"
                            + attributeName
                            + "' to float because it is type "
                            + type );
                }
            }
        }

        throw new IllegalArgumentException( "No '" + attributeName
                                            + "' attribute found for variable '"
                                            + ncVariable + " in netCDF data." );
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
        List<Attribute> variableAttributes = ncVariable.getAttributes();

        for ( Attribute attribute : variableAttributes )
        {
            if ( attribute.getShortName()
                          .toLowerCase()
                          .equals( attributeName.toLowerCase() ) )
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
                    return attribute.getNumericValue()
                                    .intValue();
                }
                else
                {
                    throw new CastMayCauseBadConversionException(
                            "Unable to convert attribute '"
                            + attributeName
                            + "' to integer because it is type "
                            + type );
                }
            }
        }

        throw new IllegalArgumentException( "No '" + attributeName
                                            + "' attribute found for variable '"
                                            + ncVariable + " in netCDF data." );
    }



    /**
     * Read TimeSerieses from across several netCDF single-validdatetime files.
     * @param featureIds The NWM feature IDs to read.
     * @param variableName The NWM variable name.
     * @param unitName The unit of all variable values.
     * @return a map of feature id to TimeSeries containing the events, may be
     * empty when no feature ids given were found in the NWM Data.
     */

    Map<Integer,TimeSeries<?>> readTimeSerieses( int[] featureIds,
                                                 String variableName,
                                                 String unitName )
            throws InterruptedException, ExecutionException
    {
        BlockingQueue<Future<NWMDoubleReadOutcome>> reads =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        CountDownLatch startGettingResults = new CountDownLatch(
                CONCURRENT_READS );
        Map<Integer,SortedSet<Event<Double>>> events = new HashMap<>( featureIds.length );

        for ( int featureId : featureIds )
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
                List<EventForNWMFeature<Double>> read = outcome.getData();
                this.featuresNotFound.addAll( outcome.getFeaturesNotFound() );

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
            List<EventForNWMFeature<Double>> read = outcome.getData();
            this.featuresNotFound.addAll( outcome.getFeaturesNotFound() );

            for ( EventForNWMFeature<Double> event : read )
            {
                // The reads are across features, we want data by feature.
                SortedSet<Event<Double>> sortedEvents = events.get( event.getFeatureId() );
                sortedEvents.add( event.getEvent() );
            }
        }

        // Go back and remove all the entries for non-existent data
        for ( Integer notFoundFeature : this.featuresNotFound )
        {
            events.remove( notFoundFeature );
        }

        Map<Integer,TimeSeries<Double>> allTimeSerieses = new HashMap<>( featureIds.length
                                                                         - this.featuresNotFound.size() );

        // Create each TimeSeries
        for ( Map.Entry<Integer,SortedSet<Event<Double>>> series : events.entrySet() )
        {
            // TODO: use the reference datetime from actual data, not args.
            // The datetimes seem to be synchronized but this is not true for
            // analyses.
            FeatureKey feature = new FeatureKey( series.getKey()
                                                       .toString(),
                                                 null,
                                                 null,
                                                 null );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, this.getReferenceDatetime() ),
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





    private static Instant readValidDatetime( NWMProfile profile,
                                              NetcdfFile netcdfFile )
    {
        String validDatetimeVariableName = profile.getValidDatetimeVariable();
        return NWMTimeSeries.readMinutesFromEpoch( netcdfFile,
                                                   validDatetimeVariableName );
    }

    private static Instant readReferenceDatetime( NWMProfile profile,
                                                  NetcdfFile netcdfFile )
    {
        String referenceDatetimeAttributeName = profile.getReferenceDatetimeVariable();
        return NWMTimeSeries.readMinutesFromEpoch( netcdfFile,
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
                                                     + OFFSET_NAME + "': '"
                                                     + multiplierType.toString()
                                                     + "' and '"
                                                     + offsetType.toString()
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
     * Actually read nc data from a variable, targeted to given indices.
     *
     * It is OK for indices to be unsorted, e.g. for minIndex to appear anywhere
     * in indices and maxIndex to appear anywhere in indices.
     *
     * No value in indices passed may be negative.
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

        String variableName = variable.getShortName();

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

        int[] result = new int[ indices.length ];
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
            throw new PreIngestException( "Failed to read variable "
                                          + variableName
                                          + " at origin "
                                          + Arrays.toString( origin )
                                          + " and shape "
                                          + Arrays.toString( shape ),
                                          e );
        }

        if ( rawValues.length != countOfRawValuesToRead )
        {
            throw new PreIngestException(
                    "Expected to read exactly " + countOfRawValuesToRead + ""
                    + " values from variable "
                    + variableName
                    + " instead got "
                    + rawValues.length );
        }

        // Write the values to the result array. Skip past unrequested values.
        for ( int i = 0, lastRawIndex = -1; i < indices.length; i++ )
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
                int rawIndexHop = indices[i] - indices[i-1];
                int currentRawIndex = lastRawIndex + rawIndexHop;
                result[i] = rawValues[currentRawIndex];
                lastRawIndex = currentRawIndex;
            }
        }

        LOGGER.debug( "Asked variable {} for range {} through {} to get values at indices {}, got raw count {}, distilled to {}",
                      variableName, minIndex, maxIndex, indices, rawValues.length, result );
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
            return rawValue * attributes.getMultiplier32()
                   + attributes.getOffsetToAdd32();
        }

        return rawValue * attributes.getMultiplier64()
               + attributes.getOffsetToAdd64();
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

        try
        {
            Array allValidDateTimes = ncVariable.read();
            int minutesSinceEpoch = allValidDateTimes.getInt( 0 );
            Duration durationSinceEpoch = Duration.ofMinutes( minutesSinceEpoch );
            return Instant.ofEpochSecond( durationSinceEpoch.toSeconds() );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to read Instant for variable "
                                          + ncVariable
                                          + " from netCDF file "
                                          + netcdfFile );
        }
    }

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
            this.readExecutor.awaitTermination( 100, TimeUnit.MILLISECONDS );
        }
        catch ( InterruptedException ie )
        {
            List<Runnable> abandoned = this.readExecutor.shutdownNow();
            LOGGER.warn( "{} shutdown interrupted, abandoned tasks: {}",
                         this.readExecutor,
                         abandoned,
                         ie );
            Thread.currentThread().interrupt();
        }

        if ( !this.readExecutor.isShutdown() )
        {
            List<Runnable> abandoned = this.readExecutor.shutdownNow();
            LOGGER.warn( "{} did not shut down quickly, abandoned tasks: {}",
                         this.readExecutor, abandoned );
        }
    }

    /**
     * Attributes that have been actually read from a netCDF Variable.
     * The purpose of this class is to help avoid re-reading the same metadata.
     */

    private static final class VariableAttributes
    {
        private final int missingValue;
        private final int fillValue;

        /** True means use float multiplier and offset, false means double */
        private final boolean has32BitPacking;
        private final float multiplier32;
        private final float offsetToAdd32;
        private final double multiplier64;
        private final double offsetToAdd64;

        VariableAttributes( int missingValue,
                            int fillValue,
                            boolean has32BitPacking,
                            float multiplier32,
                            float offsetToAdd32,
                            double multiplier64,
                            double offsetToAdd64 )
        {
            this.missingValue = missingValue;
            this.fillValue = fillValue;
            this.has32BitPacking = has32BitPacking;
            this.multiplier32 = multiplier32;
            this.offsetToAdd32 = offsetToAdd32;
            this.multiplier64 = multiplier64;
            this.offsetToAdd64 = offsetToAdd64;
        }

        int getMissingValue()
        {
            return this.missingValue;
        }

        int getFillValue()
        {
            return this.fillValue;
        }

        boolean has32BitPacking()
        {
            return this.has32BitPacking;
        }

        float getMultiplier32()
        {
            if ( !this.has32BitPacking )
            {
                throw new IllegalStateException( "This instance has 64-bit packing, use getMultiplier64()" );
            }

            return this.multiplier32;
        }

        float getOffsetToAdd32()
        {
            if ( !this.has32BitPacking )
            {
                throw new IllegalStateException( "This instance has 64-bit packing, use getOffsetToAdd64()" );
            }

            return this.offsetToAdd32;
        }

        double getMultiplier64()
        {
            if ( this.has32BitPacking )
            {
                throw new IllegalStateException( "This instance has 32-bit packing, use getMultiplier32()" );
            }

            return this.multiplier64;
        }

        double getOffsetToAdd64()
        {
            if ( this.has32BitPacking )
            {
                throw new IllegalStateException( "This instance has 32-bit packing, use getOffsetToAdd32()" );
            }

            return this.offsetToAdd64;
        }
    }


    /**
     * Task that performs NetcdfFile.open on given URI.
     */

    private static final class NWMResourceOpener implements Callable<NetcdfFile>
    {
        private final URI uri;

        NWMResourceOpener( URI uri )
        {
            Objects.requireNonNull( uri );
            this.uri = uri;
        }

        @Override
        public NetcdfFile call() throws IOException
        {
            return NetcdfFile.open( this.uri.toString() );
        }
    }

    private static final class EventForNWMFeature<T>
    {
        private static final int NO_MEMBER = Integer.MIN_VALUE;
        private final int featureId;
        private final Event<T> event;
        private final int ensembleMemberNumber;

        EventForNWMFeature( int featureId, Event<T> event )
        {
            this( featureId, event, NO_MEMBER );
        }

        EventForNWMFeature( int featureId, Event<T> event, int ensembleMemberNumber )
        {
            Objects.requireNonNull( event );
            this.featureId = featureId;
            this.event = event;
            this.ensembleMemberNumber = ensembleMemberNumber;
        }

        int getFeatureId()
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


    private static final class NWMDoubleReadOutcome
    {
        private final List<EventForNWMFeature<Double>> data;
        private final Set<Integer> featuresNotFound;

        NWMDoubleReadOutcome( List<EventForNWMFeature<Double>> data,
                              Set<Integer> featuresNotFound )
        {
            this.data = data;
            this.featuresNotFound = featuresNotFound;
        }

        List<EventForNWMFeature<Double>> getData()
        {
            return this.data;
        }

        Set<Integer> getFeaturesNotFound()
        {
            return this.featuresNotFound;
        }
    }

    /**
     * Task that reads a single event from given NWM profile, variables, etc.
     */

    private static final class NWMDoubleReader implements Callable<NWMDoubleReadOutcome>
    {

        private static final int NOT_FOUND = Integer.MIN_VALUE;
        private final NWMProfile profile;
        private final NetcdfFile netcdfFile;
        private final int[] featureIds;
        private final String variableName;
        private final Instant originalReferenceDatetime;
        private final NWMFeatureCache featureCache;
        private final boolean isEnsemble;
        private final Set<Integer> featuresNotFound;

        NWMDoubleReader( NWMProfile profile,
                         NetcdfFile netcdfFile,
                         int[] featureIds,
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

        private NWMDoubleReadOutcome readDoubles( NWMProfile profile,
                                                  NetcdfFile netcdfFile,
                                                  int[] featureIds,
                                                  String variableName,
                                                  Instant originalReferenceDatetime,
                                                  NWMFeatureCache featureCache,
                                                  boolean isEnsemble )
        {
            // Get the valid datetime
            Instant validDatetime = NWMTimeSeries.readValidDatetime( profile,
                                                                     netcdfFile );

            // Get the reference datetime
            Instant ncReferenceDatetime = NWMTimeSeries.readReferenceDatetime( profile,
                                                                               netcdfFile );

            // Validate: this referenceDatetime should match what was set originally,
            // except for analysis/assim data.
            if ( !profile.getNwmConfiguration()
                         .toLowerCase()
                         .contains( "analysis" )
                 && !ncReferenceDatetime.equals( originalReferenceDatetime ) )
            {
                throw new PreIngestException( "The reference datetime "
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

            List<FeatureIdWithItsIndex> features = new ArrayList<>( featureIds.length );

            // Discover the minimum and maximum indexes requested while getting
            // them from the featureCache in order to know the nc range to read.
            // Initialize to extreme values to detect when nothing was found
            // and to have some assurance that "less than" and "greater than"
            // will work from the start of the loop.
            int minIndex = Integer.MAX_VALUE;
            int maxIndex = Integer.MIN_VALUE;

            for ( int featureId : featureIds )
            {
                FeatureIdWithItsIndex feature;
                int indexOfFeature = featureCache.findFeatureIndex( profile,
                                                                    netcdfFile,
                                                                    featureId );
                feature = new FeatureIdWithItsIndex( featureId, indexOfFeature );
                features.add( feature );

                if ( !feature.found() )
                {
                    this.featuresNotFound.add( feature.getFeatureId() );
                    continue;
                }

                if ( feature.getIndex() < minIndex )
                {
                    minIndex = feature.getIndex();
                }

                if ( feature.getIndex() > maxIndex )
                {
                    maxIndex = feature.getIndex();
                }
            }

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
                                              .mapToInt( FeatureIdWithItsIndex::getIndex )
                                              .toArray();
            int[] companionFeatures = features.stream()
                                              .filter( FeatureIdWithItsIndex::found )
                                              .mapToInt( FeatureIdWithItsIndex::getFeatureId )
                                              .toArray();

            Variable variableVariable =  netcdfFile.findVariable( variableName );
            int[] rawVariableValues;


            // Preserve the context of which netCDF blob is read with try/catch.
            // (The readRawInts method does not need the netCDF blob.)
            try
            {
                rawVariableValues = NWMTimeSeries.readRawInts( variableVariable,
                                                               indicesOfFeatures,
                                                               minIndex,
                                                               maxIndex );
                LOGGER.debug( "Read integer values {} corresponding to feature ids {} at indices {} from {}",
                              rawVariableValues, companionFeatures, indicesOfFeatures, netcdfFile.getLocation() );
            }
            catch ( PreIngestException pie )
            {
                throw new PreIngestException( "While reading netCDF data at "
                                              + netcdfFile.getLocation(), pie );
            }

            VariableAttributes attributes = NWMTimeSeries.readVariableAttributes( variableVariable );

            List<EventForNWMFeature<Double>> list = new ArrayList<>( rawVariableValues.length );

            int missingValue = attributes.getMissingValue();
            int fillValue = attributes.getFillValue();

            for ( int i = 0; i < rawVariableValues.length; i++ )
            {
                double variableValue;

                if ( rawVariableValues[i] == missingValue
                     || rawVariableValues[i] == fillValue )
                {
                    LOGGER.debug( "Found missing value {} (one of {}, {}) at index {} for feature {} in variable {} of netCDF {}",
                                  rawVariableValues[i], missingValue, fillValue, i, companionFeatures[i], variableName, netcdfFile.getLocation() );
                    variableValue = MissingValues.DOUBLE;
                }
                else
                {
                    // Unpack.
                    variableValue = NWMTimeSeries.unpack( rawVariableValues[i],
                                                          attributes );
                }

                Event<Double> event = Event.of( validDatetime, variableValue );

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
                          rawVariableValues, variableName, validDatetime, list, netcdfFile );

            return new NWMDoubleReadOutcome( Collections.unmodifiableList( list ),
                                             Collections.unmodifiableSet( this.featuresNotFound ) );
        }

        /**
         * Reads and validates the ensemble number from an NWM netCDF resource.
         *
         * Assumes that NWM netCDF resources only have one ensemble number in
         * the global attributes.
         *
         * @param profile The profile describing the netCDF dataset.
         * @param netcdfFile The individual netCDF resource to read.
         * @return The member number from the global attributes.
         * @throws PreIngestException When ensemble member attribute is missing.
         * @throws PreIngestException When ensemble number exceeds count in profile.
         * @throws PreIngestException When ensemble number is less than 1.
         */
        private int readEnsembleNumber( NWMProfile profile,
                                        NetcdfFile netcdfFile )
        {

            int memberCount = profile.getMemberCount();

            // Get the ensemble_member_number
            String memberNumberAttributeName = profile.getMemberAttribute();
            List<Attribute> globalAttributes = netcdfFile.getGlobalAttributes();

            int ncEnsembleNumber = NOT_FOUND;

            for ( Attribute globalAttribute : globalAttributes )
            {
                if ( globalAttribute.getShortName()
                                    .equals( memberNumberAttributeName ) )
                {
                    ncEnsembleNumber = globalAttribute.getNumericValue()
                                                      .intValue();
                    break;
                }
            }

            if ( ncEnsembleNumber == NOT_FOUND )
            {
                throw new PreIngestException( "Could not find ensemble member attribute "
                                              + memberNumberAttributeName +
                                              " in netCDF file "
                                              + netcdfFile );
            }

            if ( ncEnsembleNumber > memberCount )
            {
                throw new PreIngestException( "Ensemble number "
                                              + ncEnsembleNumber
                                              + " unexpectedly exceeds member count "
                                              + memberCount );
            }

            if ( ncEnsembleNumber < 1 )
            {
                throw new PreIngestException( "Ensemble number "
                                              + ncEnsembleNumber
                                              + " is unexpectedly less than 1." );
            }

            return ncEnsembleNumber;
        }

        Set<Integer> getFeaturesNotFound()
        {
            return Collections.unmodifiableSet( this.featuresNotFound );
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
         * The contents of the feature variable for a given netCDF resource.
         *
         * This is a performance optimization (avoid re-reading the features).
         * GuardedBy featureCacheGuard
         *
         * This is kept in int[] form in order to validate new netCDF resources.
         */
        private int[] originalFeatures;

        /**
         * Whether the features stored is sorted. Allows binary search if true.
         *
         * Only one Thread should write this value after construction, the rest
         * read it.
         *
         * This is a performance optimization (avoid linear search of features).
         *
         * Not certain that this needs to be volatile, but just in case...
         *
         * GuardedBy featureCacheGuard
         */
        private volatile boolean isSorted = false;

        /**
         * First Thread to set to true wins
         * and gets to write to the featureCache. Everyone else reads.
         */
        private final AtomicBoolean
                featureCacheRaceResolver = new AtomicBoolean( false );

        /**
         * Guards featureCache variable.
         */
        private final CountDownLatch featureCacheGuard = new CountDownLatch( 1 );

        /**
         * Tracks which netCDF resoruces have had their features validated.
         */
        private final Map<NetcdfFile,AtomicBoolean> validatedFeatures;


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
         * given NWMProfile and this feature cache.
         * @param profile The profile to use (has name of feature variable).
         * @param netcdfFile The netCDF blob to search.
         * @param featureId The NWM feature id to search for.
         * @return The index of the featureID within the feature variable, or
         * a negative integer when not found.
         */

        private int findFeatureIndex( NWMProfile profile,
                                      NetcdfFile netcdfFile,
                                      int featureId )
        {
            int[] features = this.readFeaturesAndCacheOrGetFeaturesFromCache( profile,
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
         * Read features and cache in instance or read from the cache. Allows us to
         * avoid multiple reads of same data.
         * @param profile
         * @param netcdfFile
         * @return the raw 1D integer array of features, in original positions.
         * Performs safe publication of an internal map, assumes internal
         * caller, assumes that internal caller will not publish the map,
         * assumes internal caller will only read from the map.
         * @throws PreIngestException When timed out waiting on cache write.
         */

        private int[] readFeaturesAndCacheOrGetFeaturesFromCache( NWMProfile profile,
                                                                  NetcdfFile netcdfFile )
        {
            String netcdfFileName = netcdfFile.getLocation();

            // Check to see if this netcdf file has features already read
            AtomicBoolean validated = this.validatedFeatures.get( netcdfFile );

            if ( validated.get() )
            {
                LOGGER.trace( "Already read netCDF resource {}, returning cached.",
                              netcdfFileName );
                try
                {
                    Duration timeout = Duration.ofMinutes( 10 );
                    boolean timedOut = !this.featureCacheGuard.await( timeout.toSeconds(),
                                                                      TimeUnit.SECONDS );
                    if ( timedOut )
                    {
                        throw new PreIngestException(
                                "While reading "
                                + netcdfFileName,
                                new TimeoutException(
                                        "Timed out waiting for feature cache after "
                                        + timeout )
                        );
                    }
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.warn( "Interrupted while waiting for feature cache to be written", ie );
                    Thread.currentThread().interrupt();
                }

                return this.originalFeatures;
            }
            else
            {
                LOGGER.trace( "Did not yet read netCDF resource {}",
                              netcdfFileName );
            }

            String featureVariableName = profile.getFeatureVariable();
            Variable featureVariable = netcdfFile.findVariable( featureVariableName );
            // Must find the location of the variable.
            // Might be nice to assume these are sorted to do a binary search,
            // but I think it is an unsafe assumption that the values are sorted
            // and we are looking for the index of the feature id to use for
            // getting a value from the actual variable needed.
            try
            {
                LOGGER.debug( "Reading features from {}", netcdfFileName );
                Array allFeatures = featureVariable.read();
                int[] unsafeFeatures = (int[]) allFeatures.get1DJavaArray( DataType.INT );

                if ( unsafeFeatures == null )
                {
                    throw new IllegalStateException( "netCDF library returned null array when looking for NWM features." );
                }

                // Clone because we may have a reference to internal nc library data
                int[] features = unsafeFeatures.clone();

                // Discover if this Thread is responsible for setting featureCache
                boolean previouslySet = this.featureCacheRaceResolver.getAndSet( true );

                if ( !previouslySet )
                {
                    LOGGER.debug( "About to set the features cache using {}",
                                  netcdfFileName );

                    // In charge of setting the feature cache, do so.
                    this.originalFeatures = features.clone();

                    // Sort the local features, compare to see if identical
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
                else
                {
                    LOGGER.debug( "About to read the features cache to compare {}",
                                  netcdfFileName );
                    // Not in charge of setting the feature cache, do a read of it.
                    int[] existingFeatures;
                    try
                    {
                        Duration timeout = Duration.ofMinutes( 10 );
                        boolean timedOut = !this.featureCacheGuard.await( timeout.toSeconds(),
                                                                          TimeUnit.SECONDS );
                        if ( timedOut )
                        {
                            throw new PreIngestException(
                                    "While reading "
                                    + netcdfFileName,
                                    new TimeoutException(
                                            "Timed out waiting for feature cache after "
                                            + timeout )
                            );
                        }
                    }
                    catch ( InterruptedException ie )
                    {
                        LOGGER.warn( "Interrupted while waiting for feature cache to be written", ie );
                        Thread.currentThread().interrupt();
                    }

                    existingFeatures = this.originalFeatures;

                    // Compare the existing features with those just read, throw an
                    // exception if they differ (by content).
                    if ( !Arrays.equals( features, existingFeatures ) )
                    {
                        throw new PreIngestException(
                                "Non-homogeneous NWM data found. The features from "
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
                throw new PreIngestException( "Failed to read features from "
                                              + netcdfFileName, ioe );
            }
        }
    }


    /**
     * An NWM feature id with its index (position) in this NWMTimeSeries.
     */

    private static final class FeatureIdWithItsIndex
    {
        private final int featureId;
        private final int index;

        FeatureIdWithItsIndex( int featureId, int index )
        {
            this.featureId = featureId;
            this.index = index;
        }

        int getFeatureId()
        {
            return this.featureId;
        }

        int getIndex()
        {
            return this.index;
        }

        boolean found()
        {
            return this.index >= 0;
        }
    }
}
