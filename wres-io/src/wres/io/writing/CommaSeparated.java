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
import java.util.Map;
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
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.ScalarOutput;
import wres.datamodel.outputs.VectorOutput;
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

                SortedMap<Integer, StringJoiner> rows =
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

    private static SortedMap<Integer, StringJoiner> getNumericRows(
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

        SortedMap<Integer, StringJoiner> rows = new TreeMap<>();

        MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> scalarOutput = null;
        MetricOutputMultiMapByTimeAndThreshold<VectorOutput> vectorOutput = null;

        try
        {
            scalarOutput = storedMetricOutput.getScalarOutput();
            vectorOutput = storedMetricOutput.getVectorOutput();
        }
        catch ( final MetricOutputAccessException e )
        {
            throw new IOException( "While getting numeric output:", e );
        }

        if ( scalarOutput != null ) // currently requiring some scalar output
        {
            SortedMap<Integer, StringJoiner> intermediate =
                    CommaSeparated.getScalarRows( scalarOutput, formatter );

            if ( vectorOutput != null )
            {
                CommaSeparated.appendVectorColumns( intermediate,
                                                    vectorOutput,
                                                    formatter );
            }

            for ( Map.Entry<Integer, StringJoiner> e : intermediate.entrySet() )
            {
                rows.put ( e.getKey(), e.getValue() );
            }
        }

        return rows;
    }

    /**
     * Get csv rows by lead time in intermediate format (StringJoiner)
     *
     * @param output data to iterate through
     * @param formatter optional formatter to format doubles with, can be null
     * @return a SortedMap
     */

    private static SortedMap<Integer, StringJoiner> getScalarRows(
            MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> output,
            DecimalFormat formatter )
    {
        SortedMap<Integer, StringJoiner> rows = new TreeMap<>();
        StringJoiner headerRow = new StringJoiner( "," );

        headerRow.add( "LEAD" + HEADER_DELIMITER + "HOUR" );
        for ( Map.Entry<MapKey<MetricConstants>,
                MetricOutputMapByTimeAndThreshold<ScalarOutput>> m
                : output.entrySet() )
        {
            String name = m.getKey().getKey().toString();

            for ( Threshold t : m.getValue().keySetByThreshold() )
            {
                String column = name + HEADER_DELIMITER + t;
                headerRow.add( column );

                for ( TimeWindow timeWindow : m.getValue().keySetByTime() )
                {
                    //TODO: allow for the full specification of a time window in the 
                    //output, including for a lead time that is not an integer number 
                    //of hours
                    int leadTime = (int) timeWindow.getLatestLeadTimeInHours();                    
                    if ( rows.get( leadTime ) == null )
                    {
                        StringJoiner row = new StringJoiner( "," );
                        row.add( Integer.toString( leadTime ) );
                        rows.put( leadTime, row );
                    }

                    StringJoiner row = rows.get( leadTime );

                    // To maintain rectangular CSV output, construct keys using
                    // both dimensions. If we do not find a value, use NA.
                    Pair<TimeWindow,Threshold> key = Pair.of( timeWindow, t );

                    ScalarOutput value = m.getValue()
                                          .get( key );

                    String toWrite = "NA";

                    if ( value != null && !value.getData().equals( Double.NaN ) )
                    {
                        if ( formatter != null )
                        {
                            toWrite = formatter.format( value.getData() );
                        }
                        else
                        {
                            toWrite = value.toString();
                        }
                    }

                    row.add( toWrite );

                }
            }
        }

        SortedMap<Integer,StringJoiner> result = new TreeMap<>();
        result.put(Integer.MIN_VALUE, headerRow);

        for ( Map.Entry<Integer, StringJoiner> e : rows.entrySet() )
        {
            result.put( e.getKey(), e.getValue() );
        }

        return result;
    }


    /**
     * Mutate existing intermediate rows by appending additional columns of
     * vector metric outputs.
     *
     * @param existingRows the existing rows to mutate - side effecting!
     * @param output the vectoroutput data to append
     * @param formatter the format to use for doubles
     */

    private static void appendVectorColumns(
            SortedMap<Integer, StringJoiner> existingRows,
            MetricOutputMultiMapByTimeAndThreshold<VectorOutput> output,
            DecimalFormat formatter )
    {
        StringJoiner headerRow = existingRows.get( Integer.MIN_VALUE );

        for ( Map.Entry<MapKey<MetricConstants>,
                MetricOutputMapByTimeAndThreshold<VectorOutput>> m
                : output.entrySet() )
        {
            Map<MetricConstants, MetricOutputMapByTimeAndThreshold<ScalarOutput>> helper
                    = DefaultDataFactory.getInstance()
                                        .getSlicer()
                                        .filterByMetricComponent( m.getValue() );

            String outerName = m.getKey().getKey().toString();

            for ( Map.Entry<MetricConstants, MetricOutputMapByTimeAndThreshold<ScalarOutput>> e
                    : helper.entrySet() )
            {
                String name = outerName + HEADER_DELIMITER + e.getKey()
                                                              .toString();

                for ( Threshold t : m.getValue().keySetByThreshold() )
                {
                    String column = name + HEADER_DELIMITER + t;
                    headerRow.add( column );

                    for ( TimeWindow timeWindow : m.getValue().keySetByTime() )
                    {
                        //TODO: assume a time window is an integer number of hours
                        //until this writer can handle more general windows
                        StringJoiner row = existingRows.get( (int) timeWindow.getLatestLeadTimeInHours() );

                        if ( row == null )
                        {
                            //TODO: make this error message generic when the 
                            //writer can handle more general time windows, 
                            //i.e. call TimeWindow.toString()
                            String message = "Expected MetricOutput to have "
                                             + "consistent dimensions between the "
                                             + "vector and scalar outputs. When "
                                             + "looking for leadtime "
                                             + timeWindow.getLatestLeadTimeInHours()
                                             + ", could not find it in scalar output.";
                            throw new IllegalStateException( message );
                        }

                        // To maintain rectangular CSV output, construct keys using
                        // both dimensions. If we do not find a value, use NA.
                        Pair<TimeWindow, Threshold> key = Pair.of( timeWindow, t );

                        ScalarOutput value = e.getValue()
                                              .get( key );

                        String toWrite = "NA";

                        if ( value != null
                             && value.getData() != null
                             && !value.getData().equals( Double.NaN ) )
                        {

                            if ( formatter != null )
                            {
                                toWrite = formatter.format( value.getData() );
                            }
                            else
                            {
                                toWrite = value.toString();
                            }
                        }

                        row.add( toWrite );
                    }
                }
            }
        }
    }
}
