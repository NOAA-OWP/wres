package wres.io.writing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.text.Format;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;
import wres.io.config.ConfigHelper;

/**
 * Helps write Comma Separated files.
 */
public class CommaSeparated
{
    private CommaSeparated()
    {
        // Prevent construction.
    }

    private static final String HEADER_DELIMITER = " ";

    /**
     * Earliest possible time window to index the header.
     */
    
    private static final TimeWindow HEADER_INDEX = TimeWindow.of( Instant.MIN,
                                                                  Instant.MIN,
                                                                  ReferenceTime.VALID_TIME,
                                                                  Duration.ofSeconds( Long.MIN_VALUE ) );
    
    /**
     * Write numerical outputs to CSV files.
     *
     * @param projectConfig the project configuration
     * @param feature the feature
     * @param storedMetricOutput the stored output
     * @throws IOException when the writing fails
     * @throws NullPointerException when any of the arguments are null
     * @throws IllegalArgumentException when destination has bad decimalFormat
     */

    public static void writeOutputFiles( ProjectConfig projectConfig,
                                         Feature feature,
                                         MetricOutputForProjectByTimeAndThreshold storedMetricOutput )
            throws IOException
    {
        Objects.requireNonNull( storedMetricOutput,
                                "Metric outputs must not be null." );
        Objects.requireNonNull( feature,
                                "The feature must not be null." );
        Objects.requireNonNull( projectConfig,
                                "The project config must not be null." );
        try
        {
            if ( projectConfig.getOutputs() == null
                 || projectConfig.getOutputs().getDestination() == null
                 || projectConfig.getOutputs().getDestination().isEmpty() )
            {
                String message = "No numeric output files specified for project.";
                throw new ProjectConfigException( projectConfig.getOutputs(),
                                                  message );
            }
            // In principle, each destination could have a different formatter, so 
            // the output must be generated separately for each destination
            List<DestinationConfig> numericalDestinations = ConfigHelper.getNumericalDestinations( projectConfig );
            for ( DestinationConfig d : numericalDestinations )
            {
                writeAllScoreOutputTypes( d, storedMetricOutput );

            }
        }
        catch ( final ProjectConfigException pce )
        {
            throw new IOException( "Please include valid numeric output clause(s) in"
                                   + " the project configuration. Example: <destination>"
                                   + "<path>c:/Users/myname/wres_output/</path>"
                                   + "</destination>",
                                   pce );
        }
    }

    /**
     * Mutates the input to append rows for all score outputs.
     *     
     * @param destinationConfig the destination configuration    
     * @param storedMetricOutput the output to use to build rows
     * @param rows the store of rows to which additional rows should be appended
     * @throws ProjectConfigException if the path for writing the output cannot be established
     * @throws IOException if the output cannot be written
     */

    private static void writeAllScoreOutputTypes( DestinationConfig destinationConfig,
                                                  MetricOutputForProjectByTimeAndThreshold storedMetricOutput )
            throws IOException, ProjectConfigException
    {     
        try
        {
            // Scores with double output
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.DOUBLE_SCORE ) )
            {
                DecimalFormat decimalFormatter = null;
                if ( destinationConfig.getDecimalFormat() != null
                     && !destinationConfig.getDecimalFormat().isEmpty() )
                {
                    decimalFormatter = new DecimalFormat();
                    decimalFormatter.applyPattern( destinationConfig.getDecimalFormat() );
                }
                CommaSeparated.writeOneScoreOutputType( destinationConfig,
                                                        storedMetricOutput.getDoubleScoreOutput(),
                                                        decimalFormatter );
            }
            // Scores with duration output
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.DURATION_SCORE ) )
            {
                // TODO: Add an optional formatter here for the duration type
                Format durationFormatter = null;
                CommaSeparated.writeOneScoreOutputType( destinationConfig,
                                                        storedMetricOutput.getDurationScoreOutput(),
                                                        durationFormatter );
            }
        }
        catch ( MetricOutputAccessException e )
        {
            throw new IOException( "While getting score output:", e );
        }       
    }

    /**
     * Mutates the input, adding rows for one score type.
     *
     * @param <T> the score component type
     * @param destinationConfig the destination configuration    
     * @param output the score output to iterate through
     * @param formatter optional formatter, can be null
     * @throws ProjectConfigException if the path for writing the output cannot be established
     * @throws IOException if the output cannot be written
     */

    private static <T extends ScoreOutput<?, T>> void writeOneScoreOutputType( DestinationConfig destinationConfig,
                                                                               MetricOutputMultiMapByTimeAndThreshold<T> output,
                                                                               Format formatter )
            throws ProjectConfigException, IOException
    {
        StringJoiner headerBase = new StringJoiner( "," );

        headerBase.add( "EARLIEST" + HEADER_DELIMITER + "TIME" )
                 .add( "LATEST" + HEADER_DELIMITER + "TIME" )
                 .add( "EARLIEST" + HEADER_DELIMITER + "LEAD" + HEADER_DELIMITER + "HOUR" )
                 .add( "LATEST" + HEADER_DELIMITER + "LEAD" + HEADER_DELIMITER + "HOUR" );
        
        // Loop across scores
        for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<T>> m : output.entrySet() )
        {
            StringJoiner headerRow = new  StringJoiner( "," );
            headerRow.merge( headerBase );
            SortedMap<TimeWindow, StringJoiner> rows =
                    getRowsForOneScore( m.getKey().getKey(), m.getValue(), headerRow, formatter );
            //Add the header row
            rows.put( HEADER_INDEX, headerRow );
            //Write the output
            writeTabularOutputToFile( destinationConfig, rows, m.getValue().getMetadata() );
        }

    }

    /**
     * Mutates the input header and rows, adding results for one score.
     *
     * @param <T> the score component type
     * @param scoreName the score name
     * @param score the score results
     * @param headerRow the header row
     * @param formatter optional formatter, can be null
     * @return the rows to write
     */

    private static <T extends ScoreOutput<?, T>> SortedMap<TimeWindow, StringJoiner>
            getRowsForOneScore( MetricConstants scoreName,
                                MetricOutputMapByTimeAndThreshold<T> score,
                                StringJoiner headerRow,
                                Format formatter )
    {
        // Slice score by components
        Map<MetricConstants, MetricOutputMapByTimeAndThreshold<T>> helper =
                DefaultDataFactory.getInstance()
                                  .getSlicer()
                                  .filterByMetricComponent( score );

        String outerName = scoreName.toString();
        SortedMap<TimeWindow, StringJoiner> returnMe = new TreeMap<>();
        // Loop across components
        for ( Entry<MetricConstants, MetricOutputMapByTimeAndThreshold<T>> e : helper.entrySet() )
        {
            // Add the component name unless there is only one component named "MAIN"
            String name = outerName;
            if ( helper.size() > 1 || ! helper.containsKey( MetricConstants.MAIN ) )
            {
                name = name + HEADER_DELIMITER + e.getKey().toString();
            }
            addRowsForOneScoreComponent( name, e.getValue(), headerRow, returnMe, formatter );
        }
        return returnMe;
    }   
    
    /**
     * Mutates the input header and rows, adding results for one score component.
     *
     * @param <T> the score component type
     * @param name the column name
     * @param component the score component results
     * @param headerRow the header row
     * @param rows the data rows
     * @param formatter optional formatter, can be null
     */

    private static <T extends ScoreOutput<?,T>> void addRowsForOneScoreComponent( String name,
                                                     MetricOutputMapByTimeAndThreshold<T> component,
                                                     StringJoiner headerRow,
                                                     SortedMap<TimeWindow, StringJoiner> rows,
                                                     Format formatter )
    {
        // Loop across the thresholds
        for ( Threshold t : component.setOfThresholdKey() )
        {
            String column = name + HEADER_DELIMITER + t;
            headerRow.add( column );
            // Loop across time windows
            for ( TimeWindow timeWindow : component.setOfTimeWindowKey() )
            {
                if ( !rows.containsKey( timeWindow ) )
                {
                    StringJoiner row = new StringJoiner( "," );
                    row.add( timeWindow.getEarliestTime().toString() );
                    row.add( timeWindow.getLatestTime().toString() );
                    row.add( Long.toString( timeWindow.getEarliestLeadTimeInHours() ) );
                    row.add( Long.toString( timeWindow.getLatestLeadTimeInHours() ) );
                    rows.put( timeWindow, row );
                }

                StringJoiner row = rows.get( timeWindow );

                // To maintain rectangular CSV output, construct keys using
                // both dimensions. If we do not find a value, use NA.
                Pair<TimeWindow, Threshold> key = Pair.of( timeWindow, t );

                T value = component.get( key );

                String toWrite = "NA";

                // Write the current score component at the current window and threshold
                if ( value != null && value.getData() != null
                     && !Double.valueOf( Double.NaN ).equals( value.getData() ) )
                {
                    if ( formatter != null )
                    {
                        toWrite = formatter.format( value.getData() );
                    }
                    else
                    {
                        toWrite = value.getData().toString();
                    }
                }

                row.add( toWrite );
            }
        }
    }       
    
    /**
     * Writes the raw tabular output to file. Uses the supplied metadata for file naming.
     * 
     * @param destinationConfig the destination configuration
     * @param rows the tabular data to write
     * @param meta the output metadata used for file naming
     * @throws ProjectConfigException if the path for writing the output cannot be established
     * @throws IOException if the output cannot be written
     */

    private static void writeTabularOutputToFile( DestinationConfig destination,
                                                  SortedMap<TimeWindow, StringJoiner> rows,
                                                  MetricOutputMetadata meta )
            throws ProjectConfigException, IOException
    {
               
        File outputDirectory = ConfigHelper.getDirectoryFromDestinationConfig( destination );
        DatasetIdentifier identifier = meta.getIdentifier();
        Path outputPath = Paths.get( outputDirectory.toString(),
                                     identifier.getGeospatialID()
                                     + "_"
                                     +meta.getMetricID().name()
                                     +"_"
                                     +identifier.getVariableID()
                                     + ".csv" );

        try ( BufferedWriter w = Files.newBufferedWriter( outputPath,
                                                          StandardCharsets.UTF_8,
                                                          StandardOpenOption.CREATE,
                                                          StandardOpenOption.TRUNCATE_EXISTING ) )
        {
            for ( StringJoiner row : rows.values() )
            {
                w.write( row.toString() );
                w.write( System.lineSeparator() );
            }
        }
    }
    
}
