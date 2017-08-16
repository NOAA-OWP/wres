package wres.io.writing;

import wres.config.ProjectConfigException;
import wres.config.generated.Conditions;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

/**
 * Helps write Comma Separated files.
 */
public class CommaSeparated
{


    /**
     * Write numerical outputs to CSV files.
     *
     * @param projectConfig the project configuration
     * @param feature the feature
     * @param storedMetricOutput the stored output
     * @throws InterruptedException when the thread is interrupted
     * @throws ExecutionException when a dependent task failed
     * @throws IOException when the writing itself fails
     * @throws ProjectConfigException when no output files are specified
     */

    public static void writeOutputFiles( ProjectConfig projectConfig,
                                         Conditions.Feature feature,
                                         MetricOutputForProjectByLeadThreshold storedMetricOutput)
            throws InterruptedException, ExecutionException, IOException, ProjectConfigException
    {
        if ( projectConfig.getOutputs() == null
             || projectConfig.getOutputs().getDestination() == null
             || projectConfig.getOutputs().getDestination().isEmpty() )
        {
            String message = "No numeric output files specified for project.";
            throw new ProjectConfigException( projectConfig.getOutputs(), message );
        }

        for ( DestinationConfig d: projectConfig.getOutputs().getDestination() )
        {
            final Map<Integer, StringJoiner> rows = new TreeMap<>();
            final StringJoiner headerRow = new StringJoiner( "," );
            headerRow.add( "LEAD_TIME" );

            if ( d.getType() == DestinationType.NUMERIC )
            {
                for ( Map.Entry<MapBiKey<MetricConstants, MetricConstants>,
                                MetricOutputMapByLeadThreshold<ScalarOutput>> m
                      : storedMetricOutput.getScalarOutput()
                                          .entrySet() )
                {
                    String name = m.getKey().getFirstKey().name();
                    String secondName = m.getKey().getSecondKey().name();

                    for ( Threshold t: m.getValue().keySetByThreshold() )
                    {
                        String column = name + "_" + secondName + "_" + t;
                        headerRow.add( column );

                        for ( MapBiKey<Integer, Threshold> key
                              : m.getValue().sliceByThreshold( t ).keySet() )
                        {
                            if ( rows.get( key.getFirstKey() ) == null )
                            {
                                StringJoiner row = new StringJoiner( "," );
                                row.add( Integer.toString( key.getFirstKey() ) );
                                rows.put( key.getFirstKey(), row );
                            }

                            StringJoiner row = rows.get( key.getFirstKey() );

                            row.add( m.getValue().get( key ).toString() );
                        }
                    }
                }
            }

            Path outputPath = Paths.get(d.getPath() + feature.getLocation().getLid() + ".csv");
            try (BufferedWriter w = Files.newBufferedWriter( outputPath,
                                                             StandardCharsets.UTF_8,
                                                             StandardOpenOption.CREATE ) )
            {
                w.write( headerRow.toString() );
                w.write( System.lineSeparator() );

                for ( StringJoiner row : rows.values() )
                {
                    w.write( row.toString() );
                    w.write( System.lineSeparator() );
                }
            }
        }
    }
}
