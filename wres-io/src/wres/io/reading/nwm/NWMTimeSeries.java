package wres.io.reading.nwm;

import java.io.Closeable;
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
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.io.reading.PreIngestException;
import wres.system.SystemSettings;

/**
 * Goal: a variable/feature/timeseries combination in a Vector NWM dataset is
 * considered a source.
 *
 * Only the variable and feature selected for an evaluation will be ingested
 * from a vector NWM dataset.
 *
 * All the NWM netCDF blobs for a given timeseries will first be found and
 * opened prior to attempting to identify the timeseries, prior to attempting
 * to ingest any rows of timeseries data.
 *
 * This class opens a set of NWM netCDF blobs as a timeseries, based on a profile.
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
     *
     * @param profile
     * @param referenceDatetime
     * @param baseUri
     * @throws NullPointerException When any argument is null.
     * @throws PreIngestException When any netCDF blob could not be opened.
     */
    NWMTimeSeries( NWMProfile profile,
                   Instant referenceDatetime,
                   URI baseUri )
    {
        Objects.requireNonNull( profile );
        Objects.requireNonNull( referenceDatetime );
        Objects.requireNonNull( baseUri );
        this.profile = profile;
        this.referenceDatetime = referenceDatetime;
        this.baseUri = baseUri;

        // Build the set of URIs based on the profile given.
        Set<URI> netcdfUris = NWMTimeSeries.getNetcdfUris( profile,
                                                           referenceDatetime,
                                                           baseUri );
        this.netcdfFiles = new HashSet<>( netcdfUris.size() );
        LOGGER.debug( "Attempting to open NWM TimeSeries with reference datetime {} and profile {} from baseUri {}.",
                      referenceDatetime, profile, baseUri );

        ThreadFactory nwmReaderThreadFactory = new BasicThreadFactory.Builder()
                .namingPattern( "NWMTimeSeries Reader" )
                .build();

        // See comments in WebSource class regarding the setup of the executor,
        // queue, and latch.
        BlockingQueue<Runnable> nwmReaderQueue =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        this.readExecutor = new ThreadPoolExecutor( CONCURRENT_READS,
                                                    CONCURRENT_READS,
                                                    SystemSettings.poolObjectLifespan(),
                                                    TimeUnit.MILLISECONDS,
                                                    nwmReaderQueue,
                                                    nwmReaderThreadFactory );
        this.readExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
        BlockingQueue<Future<NetcdfFile>> openBlobQueue =
                new ArrayBlockingQueue<>( CONCURRENT_READS );
        CountDownLatch startGettingResults =
                new CountDownLatch( CONCURRENT_READS );

        try
        {
            // Open all the relevant files during construction, or fail.
            for ( URI netcdfUri : netcdfUris )
            {
                NWMResourceOpener opener = new NWMResourceOpener( netcdfUri );
                Future<NetcdfFile> futureBlob = this.readExecutor.submit( opener );
                openBlobQueue.add( futureBlob );
                startGettingResults.countDown();

                if ( startGettingResults.getCount() <= 0 )
                {
                    NetcdfFile netcdfFile = openBlobQueue.take()
                                                         .get();
                    this.netcdfFiles.add( netcdfFile );
                }
            }

            // Finish getting the remainder of netCDF resources being opened.
            for ( Future<NetcdfFile> opening : openBlobQueue )
            {
                NetcdfFile netcdfFile = opening.get();
                this.netcdfFiles.add( netcdfFile );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while opening netCDF resources.", ie );
            this.close();
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            this.close();
            throw new PreIngestException( "Failed to open netCDF resource.",
                                          ee );
        }

        this.featureCache = new NWMFeatureCache( this.netcdfFiles );
        LOGGER.debug( "Successfully opened NWM TimeSeries with reference datetime {} and profile {} from baseUri {}.",
                      referenceDatetime, profile, baseUri );
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

            if ( profile.getMemberCount() > 1 )
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
                if ( profile.getMemberCount() > 1 )
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

    int countOfNetcdfFiles()
    {
        return this.getNetcdfFiles().size();
    }


    /** Currently only compiling, not supported yet with multiple features */
    Map<Integer,TimeSeries<?>> readEnsembleTimeSerieses( int[] featureIds,
                                                         String variableName )
    {
        final int NOT_FOUND = Integer.MIN_VALUE;

        int memberCount = this.getProfile().getMemberCount();
        int validDatetimeCount = this.getProfile().getBlobCount();


        // Map from ensemble number to map of instant to double[]
        Map<Instant,double[]> ensembleValues = new HashMap<>( memberCount );

        for ( NetcdfFile netcdfFile : this.getNetcdfFiles() )
        {
            // Get the ensemble_member_number
            String memberNumberAttributeName = this.getProfile().getMemberAttribute();
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

            NWMDoubleReader reader = new NWMDoubleReader( this.getProfile(),
                                                          netcdfFile,
                                                          featureIds,
                                                          variableName,
                                                          this.getReferenceDatetime(),
                                                          this.featureCache );
            List<EventForNWMFeature<Double>> event = reader.call();
            Instant eventDatetime = event.get( 0 )
                                         .getEvent()
                                         .getTime();
            double[] ensembleRow = ensembleValues.get( eventDatetime );

            if ( Objects.isNull( ensembleRow ) )
            {
                ensembleRow = new double[memberCount];
                ensembleValues.put( eventDatetime, ensembleRow );
            }

            ensembleRow[ncEnsembleNumber - 1] = event.get( 0 )
                                                     .getEvent()
                                                     .getValue();
        }

        if ( ensembleValues.size() != validDatetimeCount )
        {
            throw new PreIngestException( "Expected "
                                          + validDatetimeCount
                                          + " different valid datetimes but only found "
                                          + ensembleValues.size()
                                          + " in netCDF resources "
                                          + this.getNetcdfFiles() );
        }

        SortedSet<Event<Ensemble>> sortedEvents = new TreeSet<>();

        for ( Map.Entry<Instant,double[]> entry : ensembleValues.entrySet() )
        {
            Ensemble ensemble = Ensemble.of( entry.getValue() );
            Event<Ensemble> ensembleEvent = Event.of( entry.getKey(), ensemble );
            sortedEvents.add( ensembleEvent );
        }

        return Map.of( featureIds[0], TimeSeries.of( this.getReferenceDatetime(),
                                                    sortedEvents ) );
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
     * @return The String representation of the value of attribute of variable.
     * @throws IllegalArgumentException When the attribute does not exist.
     * @throws IllegalArgumentException When the type is not float or double.
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
                    throw new IllegalArgumentException( "Unable to convert attribute '"
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
     * @throws IllegalArgumentException When the type is not float or double.
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

                if ( type.equals( DataType.FLOAT ) )
                {
                    LOGGER.trace( "Promoting float to double for {}", attribute );
                    return attribute.getNumericValue()
                                    .doubleValue();
                }
                else if ( type.equals( DataType.DOUBLE ) )
                {
                    // No loss of precision when promoting float to double.
                    return attribute.getNumericValue()
                                    .doubleValue();
                }
                else
                {
                    throw new IllegalArgumentException( "Unable to convert attribute '"
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
     * @return The String representation of the value of attribute of variable.
     * @throws IllegalArgumentException When the attribute does not exist.
     * @throws IllegalArgumentException When the type cast would cause loss.
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
                    throw new IllegalArgumentException( "Unable to convert attribute '"
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
     * @return a map of feature id to TimeSeries containing the events.
     */

    Map<Integer,TimeSeries<?>> readTimeSerieses( int[] featureIds,
                                                 String variableName )
            throws InterruptedException, ExecutionException
    {
        BlockingQueue<Future<List<EventForNWMFeature<Double>>>> reads =
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
                                                          this.featureCache );
            Future<List<EventForNWMFeature<Double>>> future =
                    this.readExecutor.submit( reader );
            reads.add( future );

            startGettingResults.countDown();

            if ( startGettingResults.getCount() <= 0 )
            {
                List<EventForNWMFeature<Double>> read = reads.take()
                                                       .get();
                for ( EventForNWMFeature<Double> event : read )
                {
                    // The reads are across features, we want data by feature.
                    SortedSet<Event<Double>> sortedEvents = events.get( event.getFeatureId() );
                    sortedEvents.add( event.getEvent() );
                }
            }
        }

        // Finish getting the remainder of events being read.
        for ( Future<List<EventForNWMFeature<Double>>> reading : reads )
        {
            List<EventForNWMFeature<Double>> read = reading.get();

            for ( EventForNWMFeature<Double> event : read )
            {
                // The reads are across features, we want data by feature.
                SortedSet<Event<Double>> sortedEvents = events.get( event.getFeatureId() );
                sortedEvents.add( event.getEvent() );
            }
        }

        Map<Integer,TimeSeries<Double>> allTimeSerieses = new HashMap<>( featureIds.length );

        // Create each TimeSeries
        for ( Map.Entry<Integer,SortedSet<Event<Double>>> series : events.entrySet() )
        {
            TimeSeries<Double> timeSeries = TimeSeries.of( this.getReferenceDatetime(),
                                                           ReferenceTimeType.T0,
                                                           series.getValue() );
            allTimeSerieses.put( series.getKey(), timeSeries );
        }

        return Collections.unmodifiableMap( allTimeSerieses );
    }



    /**
     * Find the given featureId index within the given netCDF blob using the
     * given NWMProfile and feature cache.
     * @param profile The profile to use (has name of feature variable).
     * @param netcdfFile The netCDF blob to search.
     * @param featureId The NWM feature id to search for.
     * @param featureCache the NWM feature cache to search
     * @return The index of the featureID within the feature variable.
     */

    private static int findFeatureIndex( NWMProfile profile,
                                         NetcdfFile netcdfFile,
                                         int featureId,
                                         NWMFeatureCache featureCache )
    {
        final int NOT_FOUND = -1;

        int indexOfFeature = NOT_FOUND;

        // Must find the location of the variable.
        // Might be nice to assume these are sorted to do a binary search,
        // but I think it is an unsafe assumption that the values are sorted
        // and we are looking for the index of the feature id to use for
        // getting a value from the actual variable needed.
        int[] rawFeatures = featureCache.readFeaturesAndCacheOrGetFeaturesFromCache( profile,
                                                                                     netcdfFile );

        for ( int i = 0; i < rawFeatures.length; i++ )
        {
            if ( rawFeatures[i] == featureId )
            {
                indexOfFeature = i;
                break;
            }
        }

        if ( indexOfFeature == NOT_FOUND )
        {
            throw new PreIngestException( "Could not find feature id "
                                          + featureId + " in netCDF file "
                                          + netcdfFile );
        }

        return indexOfFeature;
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

        int missingValue = readAttributeAsInt( variable,
                                               "missing_value" );
        int fillValue = readAttributeAsInt( variable,
                                            "_FillValue");
        double multiplier = readAttributeAsDouble( variable,
                                                   "scale_factor" );
        double offsetToAdd = readAttributeAsDouble( variable,
                                                    "add_offset" );
        return new VariableAttributes( missingValue,
                                       fillValue,
                                       multiplier,
                                       offsetToAdd );
    }

    private static int[] readRawInts( Variable variable, int[] indices )
    {
        Objects.requireNonNull( variable );
        Objects.requireNonNull( indices );

        if ( indices.length < 1 )
        {
            throw new IllegalArgumentException( "Must pass at least one index." );
        }

        int[] origin = indices.clone();
        int[] shape = { origin.length };

        try
        {
            Array array = variable.read( origin, shape );
            int[] values = ( int[] ) array.get1DJavaArray( DataType.INT );

            if ( values.length != origin.length )
            {
                throw new PreIngestException(
                        "Expected to read exactly " + origin.length + ""
                        + " values, instead got "
                        + values.length );
            }

            return values;
        }
        catch ( IOException | InvalidRangeException e )
        {
            throw new PreIngestException( "Failed to read variable "
                                          + variable
                                          + " at origin "
                                          + Arrays.toString( origin )
                                          + " and shape "
                                          + Arrays.toString( shape ),
                                          e );
        }
    }


    /**
     * Given an integer raw (packed) value, use the given attributes to unpack.
     * @param rawValue The raw (packed) int value.
     * @param attributes The attributes associated with the variable.
     * @return The value unpacked into a double.
     */
    private static double unpack( int rawValue, VariableAttributes attributes )
    {
        return rawValue * attributes.getMultiplier()
               + attributes.getOffsetToAdd();
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

    public void close()
    {
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
        private final double multiplier;
        private final double offsetToAdd;

        VariableAttributes( int missingValue,
                            int fillValue,
                            double multiplier,
                            double offsetToAdd )
        {
            this.missingValue = missingValue;
            this.fillValue = fillValue;
            this.multiplier = multiplier;
            this.offsetToAdd = offsetToAdd;
        }

        int getMissingValue()
        {
            return this.missingValue;
        }

        int getFillValue()
        {
            return this.fillValue;
        }

        double getMultiplier()
        {
            return this.multiplier;
        }

        double getOffsetToAdd()
        {
            return this.offsetToAdd;
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
        private final int featureId;
        private final Event<T> event;

        EventForNWMFeature( int featureId, Event<T> event )
        {
            Objects.requireNonNull( event );
            this.featureId = featureId;
            this.event = event;
        }

        int getFeatureId()
        {
            return featureId;
        }

        Event<T> getEvent()
        {
            return this.event;
        }
    }


    /**
     * Task that reads a single event from given NWM profile, variables, etc.
     */

    private static final class NWMDoubleReader implements Callable<List<EventForNWMFeature<Double>>>
    {
        private final NWMProfile profile;
        private final NetcdfFile netcdfFile;
        private final int[] featureIds;
        private final String variableName;
        private final Instant originalReferenceDatetime;
        private final NWMFeatureCache featureCache;

        NWMDoubleReader( NWMProfile profile,
                         NetcdfFile netcdfFile,
                         int[] featureIds,
                         String variableName,
                         Instant originalReferenceDatetime,
                         NWMFeatureCache featureCache )
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
        }

        @Override
        public List<EventForNWMFeature<Double>> call()
        {
            return this.readDoubles( this.profile,
                                     this.netcdfFile,
                                     this.featureIds,
                                     this.variableName,
                                     this.originalReferenceDatetime,
                                     this.featureCache );
        }

        private List<EventForNWMFeature<Double>> readDoubles( NWMProfile profile,
                                                              NetcdfFile netcdfFile,
                                                              int[] featureIds,
                                                              String variableName,
                                                              Instant originalReferenceDatetime,
                                                              NWMFeatureCache featureCache )
        {
            // Get the valid datetime
            Instant validDatetime = NWMTimeSeries.readValidDatetime( profile,
                                                                     netcdfFile );

            // Get the reference datetime
            Instant ncReferenceDatetime = NWMTimeSeries.readReferenceDatetime( profile,
                                                                               netcdfFile );

            // Validate: this referenceDatetime should match what was set originally.
            // (This doesn't work for analysis_assim)
            if ( !ncReferenceDatetime.equals( originalReferenceDatetime ) )
            {
                throw new PreIngestException( "The reference datetime "
                                              + ncReferenceDatetime
                                              + " from netCDF file "
                                              + netcdfFile
                                              + " does not match expected value "
                                              + originalReferenceDatetime );
            }

            int[] indicesOfFeatures = new int[featureIds.length];

            for ( int i = 0; i < featureIds.length; i++ )
            {
                int indexOfFeature = NWMTimeSeries.findFeatureIndex( profile,
                                                                     netcdfFile,
                                                                     featureIds[i],
                                                                     featureCache );
                indicesOfFeatures[i] = indexOfFeature;
            }

            Variable variableVariable =  netcdfFile.findVariable( variableName );
            int[] rawVariableValues;

            // Preserve the context of which netCDF blob is read with try/catch.
            // (The readRawInts method does not need the netCDF blob.)
            try
            {
                rawVariableValues = NWMTimeSeries.readRawInts( variableVariable,
                                                               indicesOfFeatures );
            }
            catch ( PreIngestException pie )
            {
                throw new PreIngestException( "While reading netCDF data at "
                                              + netcdfFile.getLocation(), pie );
            }

            VariableAttributes attributes = NWMTimeSeries.readVariableAttributes( variableVariable );

            List<EventForNWMFeature<Double>> list = new ArrayList<>( rawVariableValues.length );

            for ( int i = 0; i < rawVariableValues.length; i++ )
            {
                double variableValue;

                if ( rawVariableValues[i] == attributes.getMissingValue()
                     || rawVariableValues[i] == attributes.getFillValue() )
                {
                    variableValue = MissingValues.DOUBLE;
                }
                else
                {
                    // Unpack.
                    variableValue = NWMTimeSeries.unpack( rawVariableValues[i],
                                                          attributes );
                }

                Event<Double> event = Event.of( validDatetime, variableValue );
                EventForNWMFeature<Double> eventWithFeatureId =
                        new EventForNWMFeature<>( featureIds[i], event );
                list.add( eventWithFeatureId );
            }

            LOGGER.trace( "Read raw values {} for variable {} at {} returning as values {} from {}",
                          rawVariableValues, variableName, validDatetime, list, netcdfFile );

            return Collections.unmodifiableList( list );
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
         */
        private int[] featureCache;

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
         * Read features and cache in instance or read from the cache. Allows us to
         * avoid multiple reads of same data.
         * @param profile
         * @param netcdfFile
         * @return the raw 1D integer array of features, in original positions.
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
                    this.featureCacheGuard.await();
                }
                catch ( InterruptedException ie )
                {
                    LOGGER.warn( "Interrupted while waiting for feature cache to be written", ie );
                    Thread.currentThread().interrupt();
                }

                return this.featureCache.clone();
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
                    this.featureCache = features.clone();

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
                        this.featureCacheGuard.await();
                    }
                    catch ( InterruptedException ie )
                    {
                        LOGGER.warn( "Interrupted while waiting for feature cache to be written", ie );
                        Thread.currentThread().interrupt();
                    }

                    existingFeatures = this.featureCache.clone();

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

                return features;
            }
            catch ( IOException ioe )
            {
                throw new PreIngestException( "Failed to read features from "
                                              + netcdfFileName, ioe );
            }
        }
    }
}
