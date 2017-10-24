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
import java.util.concurrent.ExecutionException;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MapBiKey;
import wres.datamodel.MapKey;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByLeadThreshold;
import wres.datamodel.MetricOutputMultiMapByLeadThreshold;
import wres.datamodel.ScalarOutput;
import wres.datamodel.Threshold;
import wres.datamodel.VectorOutput;
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
                                         MetricOutputForProjectByLeadThreshold storedMetricOutput )
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
                                             feature.getLid()
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
            MetricOutputForProjectByLeadThreshold storedMetricOutput )
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

        MetricOutputMultiMapByLeadThreshold<ScalarOutput> scalarOutput = null;
        MetricOutputMultiMapByLeadThreshold<VectorOutput> vectorOutput = null;

        try
        {
            scalarOutput = storedMetricOutput.getScalarOutput();
            vectorOutput = storedMetricOutput.getVectorOutput();
        }
        catch ( InterruptedException ie )
        {
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            throw new IOException( "While getting numeric output", ee );
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
            MetricOutputMultiMapByLeadThreshold<ScalarOutput> output,
            DecimalFormat formatter )
    {
        SortedMap<Integer, StringJoiner> rows = new TreeMap<>();
        StringJoiner headerRow = new StringJoiner( "," );

        headerRow.add( "LEAD" + HEADER_DELIMITER + "HOUR" );
        for ( Map.Entry<MapKey<MetricConstants>,
                MetricOutputMapByLeadThreshold<ScalarOutput>> m
                : output.entrySet() )
        {
            String name = m.getKey().getKey().toString();

            for ( Threshold t : m.getValue().keySetByThreshold() )
            {
                String column = name + HEADER_DELIMITER + t;
                headerRow.add( column );

                for ( Integer leadTime : m.getValue().keySetByLead() )
                {
                    if ( rows.get( leadTime ) == null )
                    {
                        StringJoiner row = new StringJoiner( "," );
                        row.add( Integer.toString( leadTime ) );
                        rows.put( leadTime, row );
                    }

                    StringJoiner row = rows.get( leadTime );

                    // To maintain rectangular CSV output, construct keys using
                    // both dimensions. If we do not find a value, use NA.
                    MapBiKey<Integer,Threshold> key =
                            DefaultDataFactory.getInstance()
                                              .getMapKey( leadTime, t );

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
            MetricOutputMultiMapByLeadThreshold<VectorOutput> output,
            DecimalFormat formatter )
    {
        StringJoiner headerRow = existingRows.get( Integer.MIN_VALUE );

        for ( Map.Entry<MapKey<MetricConstants>,
                MetricOutputMapByLeadThreshold<VectorOutput>> m
                : output.entrySet() )
        {
            Map<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> helper
                    = DefaultDataFactory.getInstance()
                                        .getSlicer()
                                        .sliceByMetricComponent( m.getValue() );

            String outerName = m.getKey().getKey().toString();

            for ( Map.Entry<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> e
                    : helper.entrySet() )
            {
                String name = outerName + HEADER_DELIMITER + e.getKey()
                                                              .toString();

                for ( Threshold t : m.getValue().keySetByThreshold() )
                {
                    String column = name + HEADER_DELIMITER + t;
                    headerRow.add( column );

                    for ( Integer leadTime : m.getValue().keySetByLead() )
                    {
                        StringJoiner row = existingRows.get( leadTime );

                        if ( row == null )
                        {
                            String message = "Expected MetricOutput to have "
                                             + "consistent dimensions between the "
                                             + "vector and scalar outputs. When "
                                             + "looking for leadtime "
                                             + leadTime
                                             + ", could not find it in scalar output.";
                            throw new IllegalStateException( message );
                        }

                        // To maintain rectangular CSV output, construct keys using
                        // both dimensions. If we do not find a value, use NA.
                        MapBiKey<Integer, Threshold> key =
                                DefaultDataFactory.getInstance()
                                                  .getMapKey( leadTime, t );

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
