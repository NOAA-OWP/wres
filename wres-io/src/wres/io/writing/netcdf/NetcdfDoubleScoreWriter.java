package wres.io.writing.netcdf;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.ma2.Array;
import ucar.ma2.ArrayChar;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayInt;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoThresholds;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.io.writing.WriteException;

/**
 * Consumes {@link DoubleScoreOutput} and writes one or more NetCDF files.
 * Only one expected to be used per project execution (as of 2018-03-13)
 */

public class NetcdfDoubleScoreWriter implements NetcdfWriter<DoubleScoreOutput>,
                                                Closeable
{
    /** The Logger */
    private static final Logger LOGGER =
            LoggerFactory.getLogger( NetcdfDoubleScoreWriter.class );

    /** The file name prefix to use if project name is not specified */
    private static final String DEFAULT_FILE_NAME = "WRES_SCORE_METRICS";

    /** The _FillValue and missing_value to use when writing. */
    private static final double DOUBLE_FILL_VALUE = Double.NaN;

    /** The _FillValue and missing_value to use when writing. */
    private static final int INT_FILL_VALUE = Integer.MIN_VALUE;

    /** The length of strings to use for string variables in the file. */
    private static final int STRING_LENGTH = 128;

    /** The locks to synchronize on when reading/writing the netCDF file */
    private final Map<NetcdfFileWriter,Object> locks;

    /** Arbitrary station_id to increment for new stations */
    private final AtomicInteger stationId;

    /** Map from feature description to the arbitrary integer assigned */
    private final ConcurrentMap<String,Integer> stations;

    /**
     * List of output files to write.
     */

    private final List<NetcdfFileWriter> files;


    /**
     * Use static utility methods "of", etc. for construction.
     * @param projectConfig the project configuration
     * @param featureCount the count of features to write (netCDF lib needs it)
     * @param timeStepCount the count of times to write (netCDF lib needs it)
     * @param leadCount the count of leads to write (netCDF lib needs it)
     * @param thresholdCount the count of thresholds to write (netCDF lib needs it)
     * @param metrics the metric names to write (netCDF lib needs it)
     */

    private NetcdfDoubleScoreWriter( ProjectConfig projectConfig,
                                     int featureCount,
                                     int timeStepCount,
                                     int leadCount,
                                     int thresholdCount,
                                     Set<MetricConstants> metrics )
            throws IOException
    {
        this.files = NetcdfDoubleScoreWriter.initializeFiles( projectConfig,
                                                              featureCount,
                                                              timeStepCount,
                                                              leadCount,
                                                              thresholdCount,
                                                              metrics );
        this.locks = new HashMap<>( this.files.size() );

        for ( NetcdfFileWriter writer : this.files )
        {
            this.locks.put( writer, new Object() );
        }

        this.stationId = new AtomicInteger( 0 );
        this.stations = new ConcurrentHashMap<>( 1 );

        LOGGER.debug( "NetcdfDoubleScoreWriter was constructed!" );
    }


    /**
     * Returns a writer of {@link DoubleScoreOutput}. Assumes that the project
     * configuration has already been validated, so that validation is the
     * caller's responsibility.
     *
     * @param projectConfig the project configuration
     * @return a writer
     * @param featureCount the count of features to write (netCDF lib needs it)
     * @param timeStepCount the count of times to write (netCDF lib needs it)
     * @param leadCount the count of lead times to write (netCDF lib needs it)
     * @param thresholdCount the count of thresholds to write (netCDF lib needs it)
     * @param metrics the metric names to write (netCDF lib needs it)
     * @throws IOException when creation or mutation of the netcdf file fails
     * @throws NullPointerException when any non-primitive arg is null
     */

    public static NetcdfDoubleScoreWriter of( ProjectConfig projectConfig,
                                              int featureCount,
                                              int timeStepCount,
                                              int leadCount,
                                              int thresholdCount,
                                              Set<MetricConstants> metrics )
            throws IOException
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( metrics );

        return new NetcdfDoubleScoreWriter( projectConfig,
                                            featureCount,
                                            timeStepCount,
                                            leadCount,
                                            thresholdCount,
                                            metrics );
    }


    /**
     * Consumes a map of {@link DoubleScoreOutput} into a NetCDF file.
     *
     * @param output the score output for one metric at several time windows and thresholds
     */

    @Override
    public void accept( MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output )
    {
        Objects.requireNonNull( output );

        // Iterate through the metrics
        for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> e : output
                .entrySet() )
        {
            // This is the metric to write
            MetricConstants myMetricToWrite = e.getKey().getKey();

            LOGGER.debug( "Metric to write {}", myMetricToWrite );

            // Right now, the following is the only information you have about the geospatial index
            // to which the output corresponds. Clearly, more information will be required, 
            // such as the geospatial coordinates, (lat,long), or grid cell index, (row,column).
            // This will need to be added to the {@link Metadata}, and set by wres-io
            // Also note that the identifier is currently optional. The geospatial index will
            // probably need to be optional too and hence tested here, exceptionally, before 
            // writing. Potentially, we could replace the string identifier for the 
            // geospatial id with a FeaturePlus. However, I am reluctant to do this with the way
            // FeaturePlus is currently defined, i.e. verbosely. Whatever we use, it 
            // must override equals and probably implement Comparable too.

            String myGeospatialIndexToWrite = e.getValue()
                                               .getMetadata()
                                               .getIdentifier()
                                               .getGeospatialID();

            // There are M metrics in the input. For each metric, the output is stored by TimeWindow (N) 
            // and Threshold (O). In principle, a DoubleScoreOutput  may contain several score components (P), 
            // but the system currently only produces scores with one component. Thus, each call to accept() 
            // will mutate MNOP layers in the NetCDF output at ONE geospatial index (at least, the way our current 
            // process-by-feature processing works.

            // TODO: mutate the netcdf file
            for ( NetcdfFileWriter writer : this.getFiles() )
            {
                try
                {
                    String variableName = myMetricToWrite.toString()
                                                         .replace( ' ', '_' );
                    LOGGER.debug( "Writing to netCDF..." );
                    Variable ncVariable = writer.findVariable( variableName );

                    if ( ncVariable != null )
                    {
                        LOGGER.debug( "ncVariable was not null." );
                        this.writeMetric( writer,
                                          myMetricToWrite,
                                          e.getValue() );
                    }
                    else
                    {
                        LOGGER.debug( "ncVariable WAS null." );
                    }

                    if ( LOGGER.isDebugEnabled() )
                    {
                        writer.flush();
                    }
                }
                catch ( IOException ioe )
                {
                    // Because java 8's function-land has trouble with checked
                    // Exceptions, use one of our unchecked ones. Feel free to
                    // change which unchecked Exception this is.
                    throw new WriteException( "Failed to write to NetCDF file.",
                                              ioe );
                }
            }
        }
    }


    @Override
    public void close()
            throws IOException
    {
        for ( NetcdfFileWriter writer : this.getFiles() )
        {
            writer.close();
        }
    }


    /**
     * Get the list of underlying files this writer is responsible for.
     * Method not anticipated to become public or protected or package-private.
     * @return the netcdf files
     */

    private List<NetcdfFileWriter> getFiles()
    {
        return this.files;
    }


    /**
     * Get the map of writers to lock objects.
     * Method not anticipated to become public or protected or package-private.
     * @return the map of writers to locks
     */

    private Map<NetcdfFileWriter,Object> getLocks()
    {
        return this.locks;
    }


    /**
     * Get the atomic integer used to generate arbitrary ids.
     * Method not anticipated to become public or protected or package-private.
     * @return the atomic integer
     */

    private AtomicInteger getStationId()
    {
        return this.stationId;
    }


    /**
     * Get the map of stations to arbitrary netCDF identifiers.
     * Method not anticipated to become public or protected or package-private.
     * @return the map of stations to ints
     */

    private ConcurrentMap<String,Integer> getStations()
    {
        return this.stations;
    }


    /**
     * Ask for a station id from a feature/station description, adding when
     * it is not already present in our map of stations to ids.
     * Method can be removed when we use something more durable than generated
     * id.
     * @param stationOrFeatureDescription a unique name for the feature
     * @return the int id to use for writing to netCDF file
     * @throws NullPointerException when any arg is null
     */

    private int getOrAddStation( String stationOrFeatureDescription )
    {
        Objects.requireNonNull( stationOrFeatureDescription );

        if ( this.getStations().containsKey( stationOrFeatureDescription ) )
        {
            return this.getStations()
                       .get( stationOrFeatureDescription );
        }

        int possiblyNewValue = this.getStationId().incrementAndGet();

        Integer result = this.getStations()
                             .putIfAbsent( stationOrFeatureDescription, possiblyNewValue );

        if ( result == null )
        {
            // Successfully put this new value
            return possiblyNewValue;
        }
        else
        {
            // Value was already present, drop our int if it hasn't moved on.
            this.getStationId().compareAndSet( possiblyNewValue, possiblyNewValue - 1 );
            return result;
        }
    }


    /**
     * Create and set up dimensions of the NetCDF files for a given project.
     * @param config the project to set up files for
     * @param featureCount the count of features to write (netCDF lib needs it)
     * @param timeStepCount the count of times to write (netCDF lib needs it)
     * @param leadCount the count of leads to write (netCDF lib needs it),
     * @param thresholdCount the count of thresholds to write (netCDF lib needs it)
     * @param metrics the metric names to write (netCDF lib needs it)
     * @return the list of files (possibly empty)
     * @throws IOException when something goes wrong when creating or writing
     * @throws NullPointerException when any non-primitive arg is null
     */

    private static List<NetcdfFileWriter> initializeFiles( ProjectConfig config,
                                                           int featureCount,
                                                           int timeStepCount,
                                                           int leadCount,
                                                           int thresholdCount,
                                                           Set<MetricConstants> metrics )
            throws IOException
    {
        Objects.requireNonNull( config );
        Objects.requireNonNull( metrics );

        int count = NetcdfDoubleScoreWriter.countNetcdfOutputFiles( config );
        List<NetcdfFileWriter> fileWriters = new ArrayList<>( count );

        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            // Return an empty list when there are no destinations.
            return Collections.unmodifiableList( fileWriters );
        }

        String projectName = config.getName();

        if ( projectName == null )
        {
            projectName = NetcdfDoubleScoreWriter.DEFAULT_FILE_NAME;
        }

        for ( DestinationConfig destinationConfig : config.getOutputs()
                                                          .getDestination() )
        {
            if ( destinationConfig.getType().equals( DestinationType.NETCDF ) )
            {
                NetcdfFileWriter writer =
                        NetcdfFileWriter.createNew( NetcdfFileWriter.Version.netcdf3,
                                                    destinationConfig.getPath()
                                                    + "/" + projectName
                                                    + ".nc" );
                writer.addGlobalAttribute( "Conventions", "CF-1.6" );
                NetcdfDoubleScoreWriter.setDimensionsAndVariables( config,
                                                                   writer,
                                                                   featureCount,
                                                                   timeStepCount,
                                                                   leadCount,
                                                                   thresholdCount,
                                                                   metrics );
                writer.create();
                writer.flush();

                fileWriters.add( writer );
            }
        }

        return Collections.unmodifiableList( fileWriters );
    }


    /**
     * Given a project config and the writer to use, set up the dimensions of
     * the netcdf file.
     * @param config the project config, not null
     * @param writer the writer to use to mutate the netcdf file, in define mode
     * @param featureCount the count of features to write (netCDF lib needs it)
     * @param timeStepCount the count of times to write (netCDF lib needs it)
     * @param leadCount the count of leads to write (netCDF lib needs it)
     * @param thresholdCount the count of thresholds to write (netCDF lib needs it)
     * @param metrics the metric names to write (netCDF lib needs it)
     * @throws IllegalStateException when writer is not in define mode
     * @throws NullPointerException when any non-primitive arg is null
     */

    private static void setDimensionsAndVariables( ProjectConfig config,
                                                   NetcdfFileWriter writer,
                                                   int featureCount,
                                                   int timeStepCount,
                                                   int leadCount,
                                                   int thresholdCount,
                                                   Set<MetricConstants> metrics )
    {
        Objects.requireNonNull( config );
        Objects.requireNonNull( writer );
        Objects.requireNonNull( metrics );

        if ( !writer.isDefineMode() )
        {
            throw new IllegalStateException(
                    "The writer must be in define mode." );
        }

        Dimension featureDimension = writer.addDimension( null,
                                                          "station",
                                                          featureCount );
        Dimension timeDimension = writer.addDimension( null,
                                                       "time",
                                                       timeStepCount );
        Dimension leadSecondsDimension = writer.addDimension( null,
                                                              "lead_seconds",
                                                              leadCount );

        Dimension thresholdDimension = writer.addDimension( null,
                                                            "threshold",
                                                            thresholdCount );

        // NetCDF 3 uses a second dimension for string variables (char[])
        Dimension stringDimension = writer.addDimension( null,
                                                         "string",
                                                         STRING_LENGTH );

        List<Dimension> featureDimensions = new ArrayList<>( 1 );
        featureDimensions.add( featureDimension );
        List<Dimension> shareableFeatureDimensions =
                Collections.unmodifiableList( featureDimensions );
        Variable featureVariable = writer.addVariable( null,
                                                       "station_id",
                                                       DataType.INT,
                                                       shareableFeatureDimensions );
        NetcdfDoubleScoreWriter.addNoDataAttributes( featureVariable,
                                                     INT_FILL_VALUE );
        Attribute featureNameAttribute =
                new Attribute( "long_name", "Station id" );
        featureVariable.addAttribute( featureNameAttribute );

        List<Dimension> thresholdDimensions = new ArrayList<>( 1 );
        thresholdDimensions.add( thresholdDimension );
        List<Dimension> shareableThresholdDimensions =
                Collections.unmodifiableList( thresholdDimensions );
        Variable thresholdVariable = writer.addVariable( null,
                                                         "threshold",
                                                         DataType.DOUBLE,
                                                         shareableThresholdDimensions );
        NetcdfDoubleScoreWriter.addNoDataAttributes( thresholdVariable,
                                                     DOUBLE_FILL_VALUE );
        Attribute thresholdCoordinatesAttribute =
                new Attribute( "coordinates", "threshold_name" );
        thresholdVariable.addAttribute( thresholdCoordinatesAttribute );

        List<Dimension> thresholdNameDimensions = new ArrayList<>( 2 );
        thresholdNameDimensions.add( thresholdDimension );
        thresholdNameDimensions.add( stringDimension );
        List<Dimension> shareableThresholdNameDimensions =
                Collections.unmodifiableList( thresholdNameDimensions );
        Variable thresholdNameVariable = writer.addVariable( null,
                                                         "threshold_name",
                                                         DataType.CHAR,
                                                         shareableThresholdNameDimensions );

        // TODO: no LONG supported by NetCDF 3, use minutes since epoch? UINT seconds since first basis time in output?
        List<Dimension> timeDimensions = new ArrayList<>( 1 );
        timeDimensions.add( timeDimension );
        List<Dimension> shareableTimeDimensions =
                Collections.unmodifiableList( timeDimensions );
        Variable timeVariable = writer.addVariable( null,
                                                    "time",
                                                    DataType.INT,
                                                    shareableTimeDimensions );
        NetcdfDoubleScoreWriter.addNoDataAttributes( timeVariable,
                                                     INT_FILL_VALUE );
        // https://www.unidata.ucar.edu/software/udunits/CHANGE_LOG implies
        // that since udunits 2.0.1 released in 2008, rfc3339 dates work.
        Attribute timeUnitsAttribute =
                new Attribute( "units", "seconds since 1970-01-01T00:00:00Z" );
        timeVariable.addAttribute( timeUnitsAttribute );

        List<Dimension> leadSecondsDimensions = new ArrayList<>( 1 );
        leadSecondsDimensions.add( leadSecondsDimension );
        List<Dimension> shareableLeadSecondsDimensions =
                Collections.unmodifiableList( leadSecondsDimensions );
        Variable leadSecondsVariable = writer.addVariable( null,
                                                           "lead_seconds",
                                                           DataType.INT,
                                                           shareableLeadSecondsDimensions );
        NetcdfDoubleScoreWriter.addNoDataAttributes( leadSecondsVariable,
                                                     INT_FILL_VALUE );
        Attribute leadSecondsUnits = new Attribute( "units", "seconds" );
        leadSecondsVariable.addAttribute( leadSecondsUnits );


        List<Dimension> scoreDimensions = new ArrayList<>( 6 );
        scoreDimensions.add( featureDimension );
        scoreDimensions.add( thresholdDimension );
        scoreDimensions.add( timeDimension );
        scoreDimensions.add( timeDimension );
        scoreDimensions.add( leadSecondsDimension );
        scoreDimensions.add( leadSecondsDimension );
        List<Dimension> shareableScoreDimensions =
                Collections.unmodifiableList( scoreDimensions );

        // The actual values we care about
        for ( MetricConstants metric : metrics )
        {
            String metricName = metric.toString();
            Variable metricVariable = writer.addVariable( null,
                                                          metricName,
                                                          DataType.DOUBLE,
                                                          shareableScoreDimensions );
            NetcdfDoubleScoreWriter.addNoDataAttributes( metricVariable,
                                                         DOUBLE_FILL_VALUE );
        }
    }


    /**
     * Sets up common "no data" or "fill value" attributes according to CF
     * conventions:
     * http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/cf-conventions.html#missing-data
     * Expected to be called exactly once per variable (not idempotent)
     * @param variable the variable to set the nodata value on, to mutate the
     *                 underlying NetCDF file
     * @param noDataValue the "fill value" or "no data value" to use
     * @throws NullPointerException when any non-primitive arg is null
     * @throws IllegalArgumentException when noDataValue is set to 0.0
     * @throws IllegalStateException when writer not in define mode
     */

    private static void addNoDataAttributes( Variable variable,
                                             double noDataValue )
    {
        Objects.requireNonNull( variable );

        if ( variable.getDataType() != DataType.DOUBLE )
        {
            throw new IllegalArgumentException( "Specify a variable of type DOUBLE when passing double as second arg" );
        }

        if ( Double.compare( noDataValue, 0.0 ) == 0 )
        {
            throw new IllegalArgumentException(
                    "Specify a noDataValue other than 0.0" );
        }

        // Transform the simple double into what nc expects (0-dimensional array?)
        double[] noDataValues = { noDataValue };
        Array ncNoDataValues = ArrayDouble.D0.makeFromJavaArray( noDataValues );

        Attribute firstAttribute =
                new Attribute( "_FillValue", DataType.DOUBLE );
        firstAttribute.setValues( ncNoDataValues );
        variable.addAttribute( firstAttribute );

        Attribute secondAttribute =
                new Attribute( "missing_value", DataType.DOUBLE );
        secondAttribute.setValues( ncNoDataValues );
        variable.addAttribute( secondAttribute );
    }


    /**
     * Sets up common "no data" or "fill value" attributes according to CF
     * conventions:
     * http://cfconventions.org/Data/cf-conventions/cf-conventions-1.7/cf-conventions.html#missing-data
     * Expected to be called exactly once per variable (not idempotent)
     * @param variable the variable to set the nodata value on, to mutate the
     *                 underlying NetCDF file
     * @param noDataValue the "fill value" or "no data value" to use
     * @throws NullPointerException when any non-primitive arg is null
     * @throws IllegalArgumentException when noDataValue is set to 0
     * @throws IllegalStateException when writer not in define mode
     */

    private static void addNoDataAttributes( Variable variable,
                                             int noDataValue )
    {
        Objects.requireNonNull( variable );

        if ( variable.getDataType() != DataType.INT )
        {
            throw new IllegalArgumentException( "Specify a variable of type INT when passing int as second arg" );
        }

        if ( noDataValue == 0 )
        {
            throw new IllegalArgumentException(
                    "Specify a noDataValue other than 0.0" );
        }

        // Transform the simple double into what nc expects (0-dimensional array?)
        int[] noDataValues = { noDataValue };
        Array ncNoDataValues = ArrayInt.D0.makeFromJavaArray( noDataValues );

        Attribute firstAttribute =
                new Attribute( "_FillValue", DataType.INT );
        firstAttribute.setValues( ncNoDataValues );
        variable.addAttribute( firstAttribute );

        Attribute secondAttribute =
                new Attribute( "missing_value", DataType.INT );
        secondAttribute.setValues( ncNoDataValues );
        variable.addAttribute( secondAttribute );
    }


    /**
     * Returns the count of output files required.
     *
     * @param config the project configuration
     * @return the number of files required
     * @throws NullPointerException when any arg is null
     */

    private static int countNetcdfOutputFiles( ProjectConfig config )
    {
        Objects.requireNonNull( config );

        int countOfNetcdfOutputs = 0;

        for ( DestinationConfig destinationConfig : config.getOutputs()
                                                          .getDestination() )
        {
            if ( destinationConfig.getType().equals( DestinationType.NETCDF ) )
            {
                countOfNetcdfOutputs++;
            }
        }

        return countOfNetcdfOutputs;
    }


    /**
     * Write the values for a collected metric to a netcdf file
     * @param writer the writer to write with
     * @param id the name/id of the metric
     * @param output the metric output map of double score outputs
     * of the writer.
     * @throws NullPointerException when any arg is null
     */

    private void writeMetric( NetcdfFileWriter writer,
                              MetricConstants id,
                              MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> output )
            throws IOException
    {
        Objects.requireNonNull( writer );
        Objects.requireNonNull( id );
        Objects.requireNonNull( output );

        // NetCDF will replace spaces with underscores in variable names.
        String variableName = id.toString().replace( ' ', '_' );
        Variable ncVariable =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer,
                                                          variableName );

        if ( ncVariable == null )
        {
            throw new IllegalArgumentException(
                    "Must set up NetCDF file to have variable "
                    + id.toString() );
        }

        // Set up features (aka 'stations' in fews-speak)
        Variable features = NetcdfDoubleScoreWriter.getVariableOrDie( writer,
                                                                      "station_id" );
        String featureName = output.getMetadata()
                                   .getIdentifier()
                                   .getGeospatialID();
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Map before: {}, this: {}", this.getStations(), this );
        }

        int featureId = this.getOrAddStation( featureName );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Feature aka station: {}, resolved id: {}, map: {}",
                          featureName, featureId, getStations() );
        }

        this.getOrAddValueToVariable( writer, features, featureId );

        // Set up thresholds
        Variable thresholds =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer, "threshold" );

        // Set up thresholds names
        Variable thresholdNames =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer, "threshold_name" );

        for ( OneOrTwoThresholds t : output.setOfThresholdKey() )
        {
            LOGGER.debug( "t: {}, t.first(): {}, t.hasTwo(): {}",
                          t, t.first(), t.hasTwo() );
            // Goal: a unique id
            double thresholdId;

            // Means to reach the goal: two numbers from the thresholds
            double firstNumber;
            double secondNumber = 0.0;

            if ( t.first().hasProbabilities() )
            {
                firstNumber = t.first().getProbabilities().first();
            }
            else
            {
                firstNumber = t.first().getValues().first();
            }

            if ( t.hasTwo() )
            {
                if ( t.second().hasProbabilities() )
                {
                    LOGGER.debug( "t.second(): {}, t.second().getProbabilities: {}",
                                  t.second(), t.second().getProbabilities() );
                    // The first of the wrapped OneOrTwoThresholds is used
                    secondNumber = t.second().getProbabilities().first();
                }
                else
                {
                    LOGGER.debug( "t.second(): {}, t.second().getValues: {}",
                                  t.second(), t.second().getValues() );
                    secondNumber = t.second().getValues().first();
                }
            }

            thresholdId = Math.pow( firstNumber, secondNumber + 1.0 );

            this.getOrAddValueToVariable( writer,
                                          thresholds,
                                          thresholdId );
            this.getOrAddValueToVariable( writer,
                                          thresholdNames,
                                          t.toString() );
        }

        // Set up times (can be used for basis time or valid time)
        Variable times =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer, "time" );

        // Set up lead seconds
        Variable leadSeconds =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer,
                                                          "lead_seconds" );

        for ( TimeWindow window : output.setOfTimeWindowKey() )
        {
            long earliestLeadTime = window.getEarliestLeadTimeInSeconds();
            long latestLeadTime = window.getLatestLeadTimeInSeconds();

            if ( earliestLeadTime < Integer.MIN_VALUE
                 || earliestLeadTime > Integer.MAX_VALUE )
            {
                throw new UnsupportedOperationException( "Can't store lead seconds value "
                                                         + earliestLeadTime
                                                         + " in an integer." );
            }

            if ( latestLeadTime < Integer.MIN_VALUE
                 || latestLeadTime > Integer.MAX_VALUE )
            {
                throw new UnsupportedOperationException( "Can't store lead seconds value "
                                                         + latestLeadTime
                                                         + " in an integer." );
            }

            this.getOrAddValueToVariable( writer,
                                          leadSeconds,
                                          (int) earliestLeadTime );

            this.getOrAddValueToVariable( writer,
                                          leadSeconds,
                                          (int) latestLeadTime );


            long earliestTime;
            long latestTime;

            Instant earliest = window.getEarliestTime();

            if ( earliest.equals( Instant.MIN ) )
            {
                // fill value is MIN_VALUE
                earliestTime = Integer.MIN_VALUE + 1;
            }
            else if ( earliest.equals( Instant.MAX ) )
            {
                earliestTime = Integer.MAX_VALUE;
            }
            else
            {
                earliestTime = earliest.getEpochSecond();
            }

            Instant latest = window.getLatestTime();

            if ( latest.equals( Instant.MAX ) )
            {
                latestTime = Integer.MAX_VALUE;
            }
            else if ( latest.equals( Instant.MIN ) )
            {
                // fill value is MIN_VALUE
                latestTime = Integer.MIN_VALUE + 1;
            }
            else
            {
                latestTime = latest.getEpochSecond();
            }

            if ( earliestTime < Integer.MIN_VALUE
                || earliestTime > Integer.MAX_VALUE )
            {
                throw new UnsupportedOperationException( "Can't store time value "
                                                         + earliestTime
                                                         + " derived from "
                                                         + earliest + " in an integer." );
            }

            if ( latestTime < Integer.MIN_VALUE
                 || latestTime > Integer.MAX_VALUE )
            {
                throw new UnsupportedOperationException( "Can't store time value "
                                                         + latestTime
                                                         + " derived from "
                                                         + latest + " in an integer." );
            }

            this.getOrAddValueToVariable( writer,
                                          times,
                                          (int) earliestTime );
            this.getOrAddValueToVariable( writer,
                                          times,
                                          (int) latestTime );
        }

        WresNetcdfVariables coordinateVariables =
                new WresNetcdfVariables( features,
                                         thresholdNames,
                                         times,
                                         leadSeconds );

        for ( Map.Entry<TimeWindow,OneOrTwoThresholds> wrappedStuff : output.keySet() )
        {
            Pair<TimeWindow,OneOrTwoThresholds> wrappedKey =
                    Pair.of( wrappedStuff.getKey(), wrappedStuff.getValue() );
            this.writeSingleValue( writer,
                                   coordinateVariables,
                                   ncVariable,
                                   output,
                                   wrappedStuff.getValue().first(),
                                   wrappedStuff.getKey(),
                                   output.get( wrappedKey ) );

            if ( wrappedStuff.getValue().second() != null )
            {
                this.writeSingleValue( writer,
                                       coordinateVariables,
                                       ncVariable,
                                       output,
                                       wrappedStuff.getValue().second(),
                                       wrappedStuff.getKey(),
                                       output.get( wrappedKey ) );
            }
        }
    }


    /**
     * Write a value to a netCDF file.
     * @param writer the writer to use
     * @param coordinateVariables the coordinate variables already found in writer
     * @param ncVariable the variable to write to (found in writer)
     * @param output TODO: make this more specific than the whole bucket.
     * @throws IOException when writing goes wrong
     * @throws NullPointerException when any arg is null
     */

    private void writeSingleValue( NetcdfFileWriter writer,
                                   WresNetcdfVariables coordinateVariables,
                                   Variable ncVariable,
                                   MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> output,
                                   Threshold threshold,
                                   TimeWindow pool,
                                   DoubleScoreOutput scoreOutput )
            throws IOException
    {
        Objects.requireNonNull( writer );
        Objects.requireNonNull( coordinateVariables );
        Objects.requireNonNull( ncVariable );
        Objects.requireNonNull( output );

        // Get the index to write to.
        int[] shape = ncVariable.getShape();

        LOGGER.debug( "Shape of {} is {}", ncVariable, shape );

        // I guess we just have to "know" this, which is OK since we set it up
        // above, but is kind of brittle.
        // 0 is station
        // 1 is threshold
        // 2 is time
        // 3 is time
        // 4 is lead seconds
        // 5 is lead seconds
        final int STATION_INDEX = 0;
        final int THRESHOLD_INDEX = 1;
        final int START_TIME_INDEX = 2;
        final int END_TIME_INDEX = 3;
        final int START_LEAD_SECONDS_INDEX = 4;
        final int END_LEAD_SECONDS_INDEX = 5;

        // I guess we need to read these to know which one to write to
        Array allStations = coordinateVariables.getFeatures()
                                               .read();
        Array allThresholds = coordinateVariables.getThresholds()
                                                 .read();
        Array allTimes = coordinateVariables.getTimes()
                                            .read();
        Array allLeadSeconds = coordinateVariables.getLeadSeconds()
                                                  .read();

        String stationName = output.getMetadata()
                                   .getIdentifier()
                                   .getGeospatialID();

        // Behold the most horrifying nested loop!
        for ( int featureIndex = 0; featureIndex < shape[STATION_INDEX]; featureIndex++ )
        {
            int currentStation = allStations.getInt( featureIndex );
            int targetStation = this.getStations().get( stationName );

            // We only enter the nested loop when station matches, and so on...
            for ( int thresholdIndex = 0;
                  currentStation == targetStation && thresholdIndex < shape[THRESHOLD_INDEX];
                  thresholdIndex++ )
            {
                char[] currentThreshold = new char[STRING_LENGTH];

                // Need to read all the chars from threshold, 2d...
                for ( int charIndex = 0; charIndex < STRING_LENGTH; charIndex++ )
                {
                    int position = STRING_LENGTH * thresholdIndex + charIndex;
                    currentThreshold[charIndex] = allThresholds.getChar( position );
                    if ( currentThreshold[charIndex] == 0x0 )
                    {
                        break;
                    }
                }

                String actualCurrentThreshold = String.valueOf( currentThreshold )
                                                      .trim(); // remove 0s.
                String targetThreshold = threshold.toString();
                LOGGER.debug( "actualCurrentThreshold: {}, targetThreshold: {}",
                              actualCurrentThreshold, targetThreshold );


                for ( int startTimeIndex = 0;
                      actualCurrentThreshold.equals( targetThreshold )
                      && startTimeIndex < shape[START_TIME_INDEX];
                      startTimeIndex++ )
                {
                    int currentStartTime = allTimes.getInt( startTimeIndex );

                    // Trust that out-of-bounds values would have failed early
                    int targetStartTime = (int) pool.getEarliestTime()
                                                    .getEpochSecond();
                    LOGGER.debug( "currentStartTime: {}, targetStartTime: {}",
                                  currentStartTime, targetStartTime );

                    for ( int endTimeIndex = 0;
                          currentStartTime == targetStartTime
                          && endTimeIndex < shape[END_TIME_INDEX];
                          endTimeIndex++ )
                    {
                        int currentEndTime = allTimes.getInt( endTimeIndex );

                        // Trust that out-of-bounds values failed early
                        int targetEndTime = (int) pool.getLatestTime()
                                                      .getEpochSecond();
                        LOGGER.debug( "currentEndTime: {}, targetEndTime: {}",
                                      currentEndTime, targetEndTime );

                        for ( int startLeadIndex = 0;
                              currentEndTime == targetEndTime
                              && startLeadIndex < shape[START_LEAD_SECONDS_INDEX];
                              startLeadIndex++ )
                        {
                            int currentStartLeadSeconds =
                                    allLeadSeconds.getInt( startLeadIndex );
                            int targetStartLeadSeconds = (int) pool.getEarliestLeadTimeInSeconds();
                            LOGGER.debug( "currentStartLeadSeconds: {}, targetStartLeadSeconds: {}",
                                          currentStartLeadSeconds, targetStartLeadSeconds );

                            for ( int endLeadIndex = 0;
                                  currentStartLeadSeconds == targetStartLeadSeconds
                                  && endLeadIndex < shape[END_LEAD_SECONDS_INDEX];
                                  endLeadIndex++ )
                            {
                                int currentEndLeadSeconds =
                                        allLeadSeconds.getInt( endLeadIndex );
                                int targetEndLeadSeconds = (int) pool.getLatestLeadTimeInSeconds();
                                LOGGER.debug( "currentEndLeadSeconds: {}, targetEndLeadSeconds: {}",
                                              currentEndLeadSeconds, targetEndLeadSeconds );

                                if ( currentEndLeadSeconds == targetEndLeadSeconds )
                                {
                                    LOGGER.debug( "Found the spot to write! {}, {}, {}, {}, {}, {}",
                                                  featureIndex,
                                                  thresholdIndex,
                                                  startTimeIndex,
                                                  endTimeIndex,
                                                  startLeadIndex,
                                                  endLeadIndex );
                                    int[] locationToWrite =
                                            { featureIndex, thresholdIndex,
                                              startTimeIndex, endTimeIndex,
                                              startLeadIndex, endLeadIndex };

                                    double targetValue = scoreOutput.getData();
                                    double[][][][][][] valueToWrite = {{{{{{ targetValue }}}}}};
                                    Array ncValueToWrite = Array.makeFromJavaArray( valueToWrite );

                                    synchronized ( this.getLocks().get( writer ) )
                                    {
                                        try
                                        {
                                            writer.write( ncVariable,
                                                          locationToWrite,
                                                          ncValueToWrite );
                                        }
                                        catch ( InvalidRangeException ire )
                                        {
                                            throw new IOException(
                                                    "Failed to write to variable "
                                                    + ncVariable
                                                    + " in NetCDF file "
                                                    + writer
                                                    + " using raw data "
                                                    + Arrays.deepToString(
                                                            valueToWrite )
                                                    + " and nc data "
                                                    + ncValueToWrite,
                                                    ire );
                                        }
                                        writer.flush();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Somewhat meaningful exception to throw when Variable is not found.
     */
    private static class VariableNotFoundException extends IllegalStateException
    {
        public VariableNotFoundException( String message )
        {
            super( message );
        }
    }


    /**
     * Get a variable from a NetcdfFileWriter or throw a meaningful exception
     * @param writer the opened writer to look inside
     * @param variableName the variable name to look for
     * @return the Variable (non-null)
     * @throws VariableNotFoundException when the variable is not found
     * @throws NullPointerException when either writer or variableName are null
     */

    private static Variable getVariableOrDie( NetcdfFileWriter writer,
                                              String variableName )
    {
        Objects.requireNonNull( writer );
        Objects.requireNonNull( variableName );

        Variable variable = writer.findVariable( variableName );

        if ( variable == null )
        {
            String message = "Could not find the variable " + variableName
                             + " in NetCDF file " + writer
                             + ", here are the  available variables: "
                             + System.lineSeparator()
                             + writer.getNetcdfFile().getVariables();
            throw new VariableNotFoundException( message );
        }

        return variable;
    }


    /**
     * Retrieve or add-and-retrieve value from 1D int variable. Idempotent.
     * @param writer the writer that variable is part of, not in define mode
     * @param variable the variable to find value in (or add to if not present)
     * @param value the value to search for within the variable
     * @return the index of the value within the variable
     * @throws IllegalArgumentException when variable is not rank 1 INT
     * @throws IllegalStateException when writer is in define mode
     * @throws IOException when something goes wrong with writing
     * @throws NullPointerException when any non-primitive arg is null
     */

    private int getOrAddValueToVariable( NetcdfFileWriter writer,
                                         Variable variable,
                                         int value )
            throws IOException
    {
        Objects.requireNonNull( writer );
        Objects.requireNonNull( variable );

        if ( !variable.getDataType().equals( DataType.INT )
             || variable.getRank() != 1 )
        {
            throw new IllegalArgumentException( "Method requires rank 1 INT variable" );
        }

        if ( writer.isDefineMode() )
        {
            throw new IllegalStateException( "Writer must not be in define mode." );
        }

        int indexToUse = INT_FILL_VALUE;
        boolean found = false;

        synchronized ( this.getLocks().get( writer ) )
        {
            Array existingValues = variable.read();

            for ( int i = 0; i < existingValues.getSize(); i++ )
            {
                if ( existingValues.getInt( i ) == value )
                {
                    // The value was found, return the index.
                    return i;
                }
                else if ( existingValues.getInt( i ) == INT_FILL_VALUE )
                {
                    // The value was not found, but this is an empty spot,
                    // so use the current spot.
                    indexToUse = i;
                    found = true;
                    break;
                }
                // Keep searching for the value.
            }

            if ( !found )
            {
                // We did not find what we were looking for...
                throw new IllegalStateException( "Could not find a way to "
                                                 + "write value " + value
                                                 + " after searching through "
                                                 + existingValues.getSize()
                                                 + " values in variable "
                                                 + variable );
            }

            // The value was not found, add and return the index.
            int[] index = { indexToUse };
            int[] rawToWrite = { value };

            // (Should this be D0 or D1? Does it matter?)
            Array ncToWrite = ArrayInt.D1.makeFromJavaArray( rawToWrite );

            try
            {
                writer.write( variable, index, ncToWrite );
            }
            catch ( InvalidRangeException ire )
            {
                throw new IOException( "Failed to write to variable "
                                       + variable + " in NetCDF file "
                                       + writer + " using raw data "
                                       + Arrays.toString( rawToWrite )
                                       + " and nc data "
                                       + ncToWrite, ire );
            }

            writer.flush();

            return indexToUse;
        }
    }


    /**
     * Retrieve or add-and-retrieve value from 1D double variable. Idempotent.
     * @param writer the writer that variable is part of, not in define mode
     * @param variable the variable to find value in (or add to if not present)
     * @param value the value to search for within the variable
     * @return the index of the value within the variable
     * @throws IOException when something goes wrong with writing
     * @throws IllegalArgumentException when variable is not rank 1 DOUBLE
     * @throws IllegalStateException when writer is in define mode
     * @throws NullPointerException when any non-primitive arg is null
     */

    private int getOrAddValueToVariable( NetcdfFileWriter writer,
                                         Variable variable,
                                         double value )
            throws IOException
    {
        Objects.requireNonNull( writer );
        Objects.requireNonNull( variable );

        if ( !variable.getDataType().equals( DataType.DOUBLE )
             || variable.getRank() != 1 )
        {
            throw new IllegalArgumentException( "Method requires rank 1 DOUBLE variable" );
        }

        if ( writer.isDefineMode() )
        {
            throw new IllegalStateException( "Writer must not be in define mode." );
        }

        LOGGER.debug( "getOrAddValueToVariable called with double value: {}, writer: {}, variable: {}",
                      value, writer, variable );

        int indexToUse = INT_FILL_VALUE;
        boolean found = false;

        synchronized ( this.getLocks().get( writer ) )
        {
            Array existingValues = variable.read();

            for ( int i = 0; i < existingValues.getSize(); i++ )
            {
                double valueAtI = existingValues.getDouble( i );

                LOGGER.trace( "double value found at index {}: {}",
                              i,
                              valueAtI );

                if ( Double.compare( valueAtI, value ) == 0 )
                {
                    LOGGER.trace( "Found existing value matches at index {}",
                                  i );
                    // The value was found, return the index.
                    return i;
                }
                else if ( Double.compare( valueAtI, DOUBLE_FILL_VALUE ) == 0 )
                {
                    LOGGER.trace( "Found a double fill value at index {}", i );
                    // The value was not found, but this is an empty spot,
                    // so use the current spot.
                    indexToUse = i;
                    found = true;
                    break;
                }
                else
                {
                    // Keep searching for the value.
                    LOGGER.trace( "Continuing to search for value {} after index {}",
                                  value, i );

                }
            }

            if ( !found )
            {
                // We did not find what we were looking for...
                throw new IllegalStateException( "Could not find a way to "
                                                 + "write value " + value
                                                 + " after searching through "
                                                 + existingValues.getSize()
                                                 + " values in variable "
                                                 + variable );
            }

            // The value was not found, add and return the index.
            int[] index = { indexToUse };
            double[] rawToWrite = { value };

            Array ncToWrite = ArrayDouble.D1.makeFromJavaArray( rawToWrite );

            try
            {
                writer.write( variable, index, ncToWrite );
            }
            catch ( InvalidRangeException ire )
            {
                throw new IOException( "Failed to write to variable "
                                       + variable + " in NetCDF file "
                                       + writer + " using raw data "
                                       + Arrays.toString( rawToWrite )
                                       + " and nc data "
                                       + ncToWrite, ire );
            }

            writer.flush();

            return indexToUse;
        }
    }


    /**
     * Retrieve or add-and-retrieve value from 1D double variable. Idempotent.
     * @param writer the writer that variable is part of, not in define mode
     * @param variable the variable to find value in (or add to if not present)
     * @param value the value to search for within the variable
     * @return the index of the value within the variable
     * @throws IOException when something goes wrong with writing
     * @throws IllegalArgumentException when variable is not rank 1 DOUBLE
     * @throws IllegalStateException when writer is in define mode
     * @throws NullPointerException when any arg is null
     */

    private int getOrAddValueToVariable( NetcdfFileWriter writer,
                                         Variable variable,
                                         String value )
            throws IOException
    {
        Objects.requireNonNull( writer );
        Objects.requireNonNull( variable );
        Objects.requireNonNull( value );

        if ( !variable.getDataType().equals( DataType.CHAR )
             || variable.getRank() != 2 )
        {
            throw new IllegalArgumentException( "Method requires rank 2 CHAR variable" );
        }

        if ( writer.isDefineMode() )
        {
            throw new IllegalStateException( "Writer must not be in define mode." );
        }

        LOGGER.debug( "getOrAddValueToVariable called with String value: {}, writer: {}, variable: {}",
                      value, writer, variable );

        int stringIndex = -1;
        boolean found = false;

        synchronized ( this.getLocks().get( writer ) )
        {
            Array existingValues = variable.read();

            // Size of existingValues will be total char count, we want one
            // char[] at a time, so divide by STRING_LENGTH.
            for ( stringIndex = 0;
                  stringIndex < existingValues.getSize() / STRING_LENGTH;
                  stringIndex++ )
            {
                char[] valueAtI = new char[STRING_LENGTH];

                // Get a single string-like char[]
                for ( int charIndex = 0; charIndex < STRING_LENGTH; charIndex++ )
                {
                    int flatIndex = stringIndex * STRING_LENGTH + charIndex;
                    valueAtI[charIndex] = existingValues.getChar( flatIndex );

                    if ( Character.compare( valueAtI[charIndex], '\0' ) == 0)
                    {
                        break;
                    }
                }

                String resolvedValueAtI = String.valueOf( valueAtI )
                                                .trim();

                LOGGER.trace( "String value found at index {}: {}",
                              stringIndex, resolvedValueAtI );

                if ( resolvedValueAtI.equals( value ) )
                {
                    LOGGER.trace( "Found existing value matches at index {}",
                                  stringIndex );
                    // The value was found, return the string index.
                    return stringIndex;
                }
                else if ( resolvedValueAtI.equals( "" ) )
                {
                    LOGGER.trace( "Found an empty String value at index {}",
                                  stringIndex  );
                    // Use the stringIndex, 0
                    found = true;
                    break;
                }
                else
                {
                    // Keep searching for the value.
                    LOGGER.trace( "Continuing to search for value {} after index {}",
                                  value, stringIndex );
                }
            }

            if ( !found )
            {
                // We did not find what we were looking for...
                throw new IllegalStateException( "Could not find a way to "
                                                 + "write value " + value
                                                 + " after searching through "
                                                 + existingValues.getSize()
                                                 + " values in variable "
                                                 + variable );
            }

            // The value was not found, add and return the index.
            int[] index = { stringIndex, 0 };
            char[][] rawToWrite = { Arrays.copyOf( value.toCharArray(), STRING_LENGTH ) };

            Array ncToWrite = ArrayChar.D2.makeFromJavaArray( rawToWrite );

            try
            {
                writer.write( variable, index, ncToWrite );
            }
            catch ( InvalidRangeException ire )
            {
                throw new IOException( "Failed to write to variable "
                                       + variable + " in NetCDF file "
                                       + writer + " using raw data "
                                       + Arrays.toString( rawToWrite )
                                       + " and nc data "
                                       + ncToWrite, ire );
            }

            writer.flush();

            return stringIndex;
        }
    }

    private static class WresNetcdfVariables
    {
        private final Variable features;
        private final Variable thresholds;
        private final Variable times;
        private final Variable leadSeconds;

        WresNetcdfVariables( Variable features,
                             Variable thresholds,
                             Variable times,
                             Variable leadSeconds )
        {
            this.features = features;
            this.thresholds = thresholds;
            this.times = times;
            this.leadSeconds = leadSeconds;
        }

        Variable getFeatures()
        {
            return this.features;
        }

        Variable getThresholds()
        {
            return this.thresholds;
        }

        Variable getTimes()
        {
            return this.times;
        }

        Variable getLeadSeconds()
        {
            return this.leadSeconds;
        }
    }
}
