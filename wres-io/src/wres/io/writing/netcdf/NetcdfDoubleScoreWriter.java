package wres.io.writing.netcdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.ma2.ArrayChar;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayInt;
import ucar.ma2.ArrayString;
import ucar.ma2.DataType;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.io.writing.WriteException;
import wres.io.writing.WriterHelper;

/**
 * Consumes {@link DoubleScoreOutput} and writes one or more NetCDF files.
 */

public class NetcdfDoubleScoreWriter implements NetcdfWriter<DoubleScoreOutput>, AutoCloseable
{
    /** The Logger */
    private static final Logger LOGGER = LoggerFactory.getLogger( NetcdfDoubleScoreWriter.class );

    /** The file name prefix to use if project name is not specified */
    private static final String DEFAULT_FILE_NAME = "WRES_SCORE_METRICS";

    /** The _FillValue and missing_value to use when writing. */
    private static final double FILL_AND_NO_DATA_VALUE = Double.NaN;

    /** The length of strings to use for string variables in the file. */
    private static final int STRING_LENGTH = 128;

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
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    private NetcdfDoubleScoreWriter( ProjectConfig projectConfig,
                                     int featureCount,
                                     int timeStepCount,
                                     int leadCount,
                                     int thresholdCount,
                                     List<String> metrics )
            throws ProjectConfigException, IOException
    {
        WriterHelper.validateProjectForWriting( projectConfig );
        this.files = NetcdfDoubleScoreWriter.initializeFiles( projectConfig,
                                                              featureCount,
                                                              timeStepCount,
                                                              leadCount,
                                                              thresholdCount,
                                                              metrics );
    }


    /**
     * Returns a writer of {@link DoubleScoreOutput}.
     * 
     * @param projectConfig the project configuration
     * @return a writer
     * @param featureCount the count of features to write (netCDF lib needs it)
     * @param timeStepCount the count of times to write (netCDF lib needs it)
     * @param leadCount the count of lead times to write (netCDF lib needs it)
     * @param thresholdCount the count of thresholds to write (netCDF lib needs it)
     * @param metrics the metric names to write (netCDF lib needs it)
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws IOException when creation or mutation of the netcdf file fails
     */

    public static NetcdfDoubleScoreWriter of( ProjectConfig projectConfig,
                                              int featureCount,
                                              int timeStepCount,
                                              int leadCount,
                                              int thresholdCount,
                                              List<String> metrics )
            throws ProjectConfigException, IOException
    {
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

        // Iterate through the metrics
        for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> e : output.entrySet() )
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

            String myGeospatialIndexToWrite = e.getValue().getMetadata().getIdentifier().getGeospatialID();

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
                                                         .replace( ' ', '_');
                    LOGGER.debug( "Writing to netCDF..." );
                    Variable ncVariable = writer.findVariable( variableName );

                    if ( ncVariable != null )
                    {
                        LOGGER.debug( "ncVariable was not null." );
                        NetcdfDoubleScoreWriter.writeMetric( writer,
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
     * It is not anticipated to become public or protected or package-private.
     * @return the netcdf files
     */

    private List<NetcdfFileWriter> getFiles()
    {
        return this.files;
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
     * @throws NullPointerException when ProjectConfig is null
     */

    private static List<NetcdfFileWriter> initializeFiles( ProjectConfig config,
                                                           int featureCount,
                                                           int timeStepCount,
                                                           int leadCount,
                                                           int thresholdCount,
                                                           List<String> metrics )
            throws IOException
    {
        Objects.requireNonNull( config );
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

        for ( DestinationConfig destinationConfig : config.getOutputs().getDestination() )
        {
            if ( destinationConfig.getType().equals( DestinationType.NETCDF ) )
            {
                NetcdfFileWriter writer =
                        NetcdfFileWriter.createNew( NetcdfFileWriter.Version.netcdf3,
                                                    destinationConfig.getPath()
                                                    + "/" + projectName + ".nc" );
                NetcdfDoubleScoreWriter.setDimensionsAndVariables( config,
                                                                   writer,
                                                                   featureCount,
                                                                   timeStepCount,
                                                                   leadCount,
                                                                   thresholdCount,
                                                                   metrics );
                writer.create();

                if ( LOGGER.isDebugEnabled() )
                {
                    writer.flush();
                }

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
     * @throws NullPointerException when any arg is null
     */

    private static void setDimensionsAndVariables( ProjectConfig config,
                                                   NetcdfFileWriter writer,
                                                   int featureCount,
                                                   int timeStepCount,
                                                   int leadCount,
                                                   int thresholdCount,
                                                   List<String> metrics )
    {
        Objects.requireNonNull( config );
        Objects.requireNonNull( writer );

        if ( !writer.isDefineMode() )
        {
            throw new IllegalStateException( "The writer must be in define mode." );
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
        Attribute featureNameAttribute = new Attribute( "long_name", "Station id" );
        featureVariable.addAttribute( featureNameAttribute );

        List<Dimension> thresholdDimensions = new ArrayList<>( 2 );
        thresholdDimensions.add( thresholdDimension );
        thresholdDimensions.add( stringDimension );
        List<Dimension> shareableThresholdDimensions =
                Collections.unmodifiableList( thresholdDimensions );
        Variable thresholdVariable = writer.addVariable( null,
                                                         "threshold",
                                                         DataType.CHAR,
                                                         shareableThresholdDimensions );

        // TODO: no LONG supported by NetCDF 3, use minutes since epoch? UINT seconds since first basis time in output?
        List<Dimension> timeDimensions = new ArrayList<>( 1 );
        timeDimensions.add( timeDimension );
        List<Dimension> shareableTimeDimensions =
                Collections.unmodifiableList( timeDimensions );
        Variable timeVariable = writer.addVariable( null,
                                                    "time",
                                                    DataType.INT,
                                                    shareableTimeDimensions );
        Attribute timeUnitsAttribute = new Attribute( "units", "seconds since 1970-01-01T00:00:00Z" );
        timeVariable.addAttribute( timeUnitsAttribute );

        List<Dimension> leadSecondsDimensions = new ArrayList<>( 1 );
        leadSecondsDimensions.add( leadSecondsDimension );
        List<Dimension> shareableLeadSecondsDimensions =
                Collections.unmodifiableList( leadSecondsDimensions );
        Variable leadSecondsVariable = writer.addVariable( null,
                                                           "lead_seconds",
                                                           DataType.INT,
                                                           shareableLeadSecondsDimensions );
        Attribute leadSecondsUnits = new Attribute( "units", "seconds" );
        timeVariable.addAttribute( leadSecondsUnits );


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
        for ( String metricName : metrics )
        {
            Variable metricVariable = writer.addVariable( null,
                                                          metricName,
                                                          DataType.DOUBLE,
                                                          shareableScoreDimensions );
            NetcdfDoubleScoreWriter.addNoDataAttributesDouble( metricVariable,
                                                               FILL_AND_NO_DATA_VALUE );
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
     * @throws NullPointerException when any arg is null
     * @throws IllegalArgumentException when noDataValue is set to 0.0
     * @throws IllegalStateException when writer not in define mode
     */
    private static void addNoDataAttributesDouble( Variable variable,
                                                   double noDataValue )
    {
        Objects.requireNonNull( variable );

        if ( Double.compare( noDataValue, 0.0 ) == 0 )
        {
            throw new IllegalArgumentException( "Specify a noDataValue other than 0.0" );
        }

        // Transform the simple double into what nc expects (0-dimensional array?)
        double[] noDataValues = { noDataValue };
        Array ncNoDataValues = ArrayDouble.D0.makeFromJavaArray( noDataValues );

        Attribute firstAttribute = new Attribute( "_FillValue", DataType.DOUBLE );
        firstAttribute.setValues( ncNoDataValues );
        variable.addAttribute( firstAttribute );

        Attribute secondAttribute = new Attribute( "missing_value", DataType.DOUBLE );
        secondAttribute.setValues( ncNoDataValues );
        variable.addAttribute( secondAttribute );
    }

    /**
     * Returns the count of output files required.
     *
     * @param config the project configuration
     * @return the number of files required
     */

    private static int countNetcdfOutputFiles( ProjectConfig config )
    {
        Objects.requireNonNull( config );


        int countOfNetcdfOutputs = 0;

        for ( DestinationConfig destinationConfig : config.getOutputs().getDestination() )
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
     */
    private static void writeMetric( NetcdfFileWriter writer,
                                     MetricConstants id,
                                     MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> output )
            throws IOException
    {
        // NetCDF will replace spaces with underscores in variable names.
        String variableName = id.toString().replace( ' ', '_' );
        Variable ncVariable =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer,
                                                          variableName );

        if ( ncVariable == null )
        {
            throw new IllegalArgumentException( "Must set up NetCDF file to have variable " + id.toString() );
        }

        // Set up features (aka 'stations' in fews-speak)
        Variable features = NetcdfDoubleScoreWriter.getVariableOrDie( writer,
                                                                      "station_id" );
        int[] featureIds = { 1 };
        Array ncFeatureIds = ArrayInt.D1.makeFromJavaArray( featureIds );

        try
        {
            writer.write( features, ncFeatureIds );
        }
        catch ( InvalidRangeException ire )
        {
            throw new IOException( "Failed to write to variable "
                                   + features + " in NetCDF file "
                                   + writer, ire );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            writer.flush();
        }

        // Set up thresholds (nonsense for now)
        Variable thresholds =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer, "threshold" );
        char[][] thresholdsValues = {
                Arrays.copyOf( "Some kind of threshold".toCharArray(), STRING_LENGTH ),
                Arrays.copyOf( "Another kind of threshold".toCharArray(), STRING_LENGTH ) };
        // Doesn't quite work, curious: (Also kind of scary that ArrayInt.D2 worked...)
        Array ncThresholdsValues = ArrayChar.D2.makeFromJavaArray( thresholdsValues );

        try
        {
            writer.write( thresholds, ncThresholdsValues );
        }
        catch ( InvalidRangeException ire )
        {
            throw new IOException( "Failed to write to variable "
                                   + thresholds + " in NetCDF file "
                                   + writer + " using raw data "
                                   + Arrays.deepToString( thresholdsValues )
                                   + " and nc data "
                                   + ncThresholdsValues, ire );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            writer.flush();
        }

        // Set up times (can be used for basis time or valid time)
        Variable times =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer, "time" );
        int[] timeValues = { 1520520000, 1520530000 };
        Array ncTimeValues = ArrayInt.D1.makeFromJavaArray( timeValues );

        try
        {
            writer.write( times, ncTimeValues );
        }
        catch ( InvalidRangeException ire )
        {
            throw new IOException( "Failed to write to variable "
                                   + times + " in NetCDF file "
                                   + writer, ire );
        }


        // Set up lead seconds (nonsense for now)
        Variable leadSeconds =
                NetcdfDoubleScoreWriter.getVariableOrDie( writer, "lead_seconds" );
        int[] leadSecondsValues = { 3600 };
        Array ncleadSecondsValues = ArrayInt.D1.makeFromJavaArray( leadSecondsValues );

        try
        {
            writer.write( leadSeconds, ncleadSecondsValues );
        }
        catch ( InvalidRangeException ire )
        {
            throw new IOException( "Failed to write to variable "
                                   + leadSeconds + " in NetCDF file "
                                   + writer + " using raw data "
                                   + Arrays.toString( leadSecondsValues )
                                   + " and nc data "
                                   + ncleadSecondsValues, ire );
        }


        if ( LOGGER.isDebugEnabled() )
        {
            writer.flush();
        }

        // Get the index to write to. How do I interrogate what's there!?
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
        Array allStations = features.read();
        Array allThresholds = thresholds.read();
        Array allTimes = times.read();
        Array allLeadSeconds = leadSeconds.read();

        // Behold the most horrifying nested loop!
        for ( int featureIndex = 0; featureIndex < shape[STATION_INDEX]; featureIndex++ )
        {
            int currentStation = allStations.getInt( featureIndex );

            // We only enter the nested loop when station matches, and so on...
            for ( int thresholdIndex = 0;
                  currentStation == 1 && thresholdIndex < shape[THRESHOLD_INDEX];
                  thresholdIndex++ )
            {

                char[] currentThreshold = new char[128];

                // Need to read all the chars from threshold, 2d...
                for ( int charIndex = 0; charIndex < STRING_LENGTH; charIndex++ )
                {
                    int position = STRING_LENGTH * thresholdIndex + charIndex;
                    currentThreshold[charIndex] = allThresholds.getChar( position );
                    LOGGER.debug( "currentThreshold: {}", currentThreshold );
                    if ( currentThreshold[charIndex] == 0x0 )
                    {
                        break;
                    }
                }

                String actualCurrentThreshold = String.valueOf( currentThreshold )
                                                      .trim(); // remove 0s.
                LOGGER.debug( "actualCurrentThreshold: {}", actualCurrentThreshold );

                for ( int startTimeIndex = 0;
                      actualCurrentThreshold.equals( "Some kind of threshold" )
                      && startTimeIndex < shape[START_TIME_INDEX];
                      startTimeIndex++ )
                {
                    int currentStartTime = allTimes.getInt( startTimeIndex );

                    for ( int endTimeIndex = 0;
                          currentStartTime == 1520520000
                          && endTimeIndex < shape[END_TIME_INDEX];
                          endTimeIndex++ )
                    {
                        int currentEndTime = allTimes.getInt( endTimeIndex );

                        for ( int startLeadIndex = 0;
                              currentEndTime == 1520530000
                              && startLeadIndex < shape[START_LEAD_SECONDS_INDEX];
                              startLeadIndex++ )
                        {
                            int currentStartLeadSeconds =
                                    allLeadSeconds.getInt( startLeadIndex );

                            for ( int endLeadIndex = 0;
                                  currentStartLeadSeconds == 3600
                                  && endLeadIndex < shape[END_LEAD_SECONDS_INDEX];
                                  endLeadIndex++ )
                            {
                                int currentEndLeadSeconds =
                                        allLeadSeconds.getInt( endLeadIndex );

                                if ( currentEndLeadSeconds == 3600 )
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
                                    double[][][][][][] valueToWrite = {{{{{{ 505.0 }}}}}};
                                    Array ncValueToWrite = Array.makeFromJavaArray( valueToWrite );

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
                                                + ncVariable + " in NetCDF file "
                                                + writer + " using raw data "
                                                + Arrays.deepToString( valueToWrite )
                                                + " and nc data "
                                                + ncValueToWrite , ire );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if ( LOGGER.isDebugEnabled() )
        {
            writer.flush();
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

}
