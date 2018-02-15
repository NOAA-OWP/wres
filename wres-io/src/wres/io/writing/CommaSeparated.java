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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
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
     * @throws IOException when the writing itself fails
     * @throws ProjectConfigException when no output files are specified
     * @throws NullPointerException when any of the arguments are null
     * @throws IllegalArgumentException when destination has bad decimalFormat
     */

    public static void writeOutputFiles( ProjectConfig projectConfig,
                                         Feature feature,
                                         MetricOutputForProjectByTimeAndThreshold storedMetricOutput )
            throws IOException, ProjectConfigException
    {
        Objects.requireNonNull( storedMetricOutput,
                                "Metric outputs must not be null." );
        Objects.requireNonNull( feature,
                                "The feature must not be null." );
        Objects.requireNonNull( projectConfig,
                                "The project config must not be null." );

        if ( projectConfig.getOutputs() == null
             || projectConfig.getOutputs().getDestination() == null
             || projectConfig.getOutputs().getDestination().isEmpty() )
        {
            String message = "No numeric output files specified for project.";
            throw new ProjectConfigException( projectConfig.getOutputs(),
                                              message );
        }

        for ( DestinationConfig d : projectConfig.getOutputs()
                                                 .getDestination() )
        {
            if ( d.getType() == DestinationType.NUMERIC )
            {
                SortedMap<TimeWindow, StringJoiner> rows = new TreeMap<>();
                
                // Add rows for all score types
                addRowsForAllScoreTypes( d, storedMetricOutput, rows );
                
                File outputDirectory = ConfigHelper.getDirectoryFromDestinationConfig( d );

                Path outputPath = Paths.get( outputDirectory.toString(),
                                             feature.getLocationId()
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
    }
    
    /**
     * Mutates the input to append rows for all score outputs.
     *     
     * @param d the destination configuration    
     * @param storedMetricOutput the output to use to build rows
     * @param rows the store of rows to which additional rows should be appended
     * @throws IOException if the rows could not be added
     */

    private static void addRowsForAllScoreTypes( DestinationConfig d,
                                                 MetricOutputForProjectByTimeAndThreshold storedMetricOutput,
                                                 SortedMap<TimeWindow, StringJoiner> rows ) throws IOException
    {     
        try
        {
            // Scores with double output
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.DOUBLE_SCORE ) )
            {
                DecimalFormat decimalFormatter = null;
                if ( d.getDecimalFormat() != null
                     && !d.getDecimalFormat().isEmpty() )
                {
                    decimalFormatter = new DecimalFormat();
                    decimalFormatter.applyPattern( d.getDecimalFormat() );
                }
                CommaSeparated.addRowsForOneScoreType( storedMetricOutput.getDoubleScoreOutput(),
                                                       decimalFormatter,
                                                       rows );
            }
            // Scores with duration output
            if ( storedMetricOutput.hasOutput( MetricOutputGroup.DURATION_SCORE ) )
            {
                // TODO: Add an optional formatter here for the duration type
                Format durationFormatter = null;
                

                CommaSeparated.addRowsForOneScoreType( storedMetricOutput.getDurationScoreOutput(),
                                                       durationFormatter,
                                                       rows );
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
     * @param output the score output to iterate through
     * @param formatter optional formatter, can be null
     * @param rows the store of output to which results should be appended
     */

    private static <T extends ScoreOutput<?,T>> void addRowsForOneScoreType(
            MetricOutputMultiMapByTimeAndThreshold<T> output,
            Format formatter,
            SortedMap<TimeWindow, StringJoiner> rows )
    {
        StringJoiner headerRow = new StringJoiner( "," );

        headerRow.add( "EARLIEST" + HEADER_DELIMITER + "TIME" )
                 .add( "LATEST" + HEADER_DELIMITER + "TIME" )
                 .add( "EARLIEST" + HEADER_DELIMITER + "LEAD" + HEADER_DELIMITER + "HOUR" )
                 .add( "LATEST" + HEADER_DELIMITER + "LEAD" + HEADER_DELIMITER + "HOUR" );
        
        // Loop across scores
        for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<T>> m : output.entrySet() )
        {
            addRowsForOneScore( m.getKey().getKey(), m.getValue(), headerRow, rows, formatter );
        }

        rows.put( HEADER_INDEX, headerRow );
    }

    /**
     * Mutates the input header and rows, adding results for one score.
     *
     * @param <T> the score component type
     * @param scoreName the score name
     * @param score the score results
     * @param headerRow the header row
     * @param rows the data rows
     * @param formatter optional formatter, can be null
     */

    private static <T extends ScoreOutput<?,T>> void addRowsForOneScore( MetricConstants scoreName,
                                            MetricOutputMapByTimeAndThreshold<T> score,
                                            StringJoiner headerRow,
                                            SortedMap<TimeWindow, StringJoiner> rows,
                                            Format formatter )
    {
        // Slice score by components
        Map<MetricConstants, MetricOutputMapByTimeAndThreshold<T>> helper =
                DefaultDataFactory.getInstance()
                                  .getSlicer()
                                  .filterByMetricComponent( score );

        String outerName = scoreName.toString();
        // Loop across components
        for ( Entry<MetricConstants, MetricOutputMapByTimeAndThreshold<T>> e : helper.entrySet() )
        {
            // Add the component name unless there is only one component named "MAIN"
            String name = outerName;
            if ( helper.size() > 1 || ! helper.containsKey( MetricConstants.MAIN ) )
            {
                name = name + HEADER_DELIMITER + e.getKey().toString();
            }
            addRowsForOneScoreComponent( name, e.getValue(), headerRow, rows, formatter );
        }
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

}
