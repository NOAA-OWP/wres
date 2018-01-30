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
import wres.datamodel.Threshold;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
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

                SortedMap<TimeWindow, StringJoiner> rows =
                        CommaSeparated.getNumericRows( d, storedMetricOutput );

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
     * Get numeric rows for a DestinationConfig
     * @param d the config to build intermediate rows from
     * @param storedMetricOutput the output to use to build rows
     * @return the rows in order
     * @throws IOException when retrieval from storedMetricOutput fails
     */

    private static SortedMap<TimeWindow, StringJoiner> getNumericRows(
            DestinationConfig d,
            MetricOutputForProjectByTimeAndThreshold storedMetricOutput )
            throws IOException
    {
        DecimalFormat formatter = null;

        if ( d.getDecimalFormat() != null
             && !d.getDecimalFormat().isEmpty() )
        {
            formatter = new DecimalFormat();
            formatter.applyPattern( d.getDecimalFormat() );
        }

        SortedMap<TimeWindow, StringJoiner> rows = new TreeMap<>();

        MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> scoreOutput = null;

        try
        {
            scoreOutput = storedMetricOutput.getScoreOutput();
        }
        catch ( final MetricOutputAccessException e )
        {
            throw new IOException( "While getting numeric output:", e );
        }

        if ( scoreOutput != null ) // currently requiring some score output
        {
            SortedMap<TimeWindow, StringJoiner> intermediate =
                    CommaSeparated.getScoreRows( scoreOutput, formatter );

            for ( Map.Entry<TimeWindow, StringJoiner> e : intermediate.entrySet() )
            {
                rows.put ( e.getKey(), e.getValue() );
            }
        }

        return rows;
    }

    /**
     * Get csv rows by lead time in intermediate format (StringJoiner).
     *
     * @param output data to iterate through
     * @param formatter optional formatter to format doubles with, can be null
     * @return a SortedMap
     */

    private static SortedMap<TimeWindow, StringJoiner> getScoreRows(
            MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output,
            DecimalFormat formatter )
    {
        SortedMap<TimeWindow, StringJoiner> rows = new TreeMap<>();
        StringJoiner headerRow = new StringJoiner( "," );

        headerRow.add( "EARLIEST" + HEADER_DELIMITER + "TIME" )
                 .add( "LATEST" + HEADER_DELIMITER + "TIME" )
                 .add( "EARLIEST" + HEADER_DELIMITER + "LEAD" + HEADER_DELIMITER + "HOUR" )
                 .add( "LATEST" + HEADER_DELIMITER + "LEAD" + HEADER_DELIMITER + "HOUR" );
        
        // Loop across scores
        for ( Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> m : output.entrySet() )
        {
            addRowsForOneScore( m.getKey().getKey(), m.getValue(), headerRow, rows, formatter );
        }

        SortedMap<TimeWindow,StringJoiner> result = new TreeMap<>();
        result.put( HEADER_INDEX, headerRow );

        for ( Entry<TimeWindow, StringJoiner> e : rows.entrySet() )
        {
            result.put( e.getKey(), e.getValue() );
        }

        return result;
    }

    /**
     * Mutates the input header and rows, adding results for one score.
     *
     * @param scoreName the score name
     * @param score the score results
     * @param headerRow the header row
     * @param rows the data rows
     * @param formatter optional formatter to format doubles with, can be null
     */

    private static void addRowsForOneScore( MetricConstants scoreName,
                                            MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> score,
                                            StringJoiner headerRow,
                                            SortedMap<TimeWindow, StringJoiner> rows,
                                            DecimalFormat formatter )
    {
        // Slice score by components
        Map<MetricConstants, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> helper =
                DefaultDataFactory.getInstance()
                                  .getSlicer()
                                  .filterByMetricComponent( score );

        String outerName = scoreName.toString();
        // Loop across components
        for ( Entry<MetricConstants, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> e : helper.entrySet() )
        {
            // Add the component name if more than one component, otherwise leave blank
            String name = outerName;
            if ( helper.size() > 1 )
            {
                name = name + HEADER_DELIMITER + e.getKey().toString();
            }
            addRowsForOneScoreComponent( name, e.getValue(), headerRow, rows, formatter );
        }
    }   
    
    /**
     * Mutates the input header and rows, adding results for one score component.
     *
     * @param name the column name
     * @param component the score component results
     * @param headerRow the header row
     * @param rows the data rows
     * @param formatter optional formatter to format doubles with, can be null
     */

    private static void addRowsForOneScoreComponent( String name,
                                                     MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> component,
                                                     StringJoiner headerRow,
                                                     SortedMap<TimeWindow, StringJoiner> rows,
                                                     DecimalFormat formatter )
    {
        // Loop across the thresholds
        for ( Threshold t : component.keySetByThreshold() )
        {
            String column = name + HEADER_DELIMITER + t;
            headerRow.add( column );
            // Loop across time windows
            for ( TimeWindow timeWindow : component.keySetByTime() )
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

                DoubleScoreOutput value = component.get( key );

                String toWrite = "NA";

                // Write the current score component at the current window and threshold
                if ( value != null && value.getData() != null && !value.getData().isNaN() )
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
