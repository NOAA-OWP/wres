package wres.io.writing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.Conditions;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MapBiKey;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByLeadThreshold;
import wres.datamodel.MetricOutputMultiMapByLeadThreshold;
import wres.datamodel.ScalarOutput;
import wres.datamodel.Threshold;

/**
 * Helps write Comma Separated files.
 */
public class CommaSeparated
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger( CommaSeparated.class );

    private CommaSeparated()
    {
        // Prevent construction.
    }

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
     * @throws NullPointerException when any of the arguments are null
     */

    public static void writeOutputFiles( ProjectConfig projectConfig,
                                         Conditions.Feature feature,
                                         MetricOutputForProjectByLeadThreshold storedMetricOutput )
            throws InterruptedException, ExecutionException, IOException,
            ProjectConfigException
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

                Path outputDirectory = Paths.get( d.getPath() );

                if ( outputDirectory == null )
                {
                    String message = "Destination path " + d.getPath() +
                                     " could not be found.";
                    throw new ProjectConfigException( d, message );
                }
                else
                {
                    File outputLocation = outputDirectory.toFile();
                    if ( outputLocation.isDirectory() )
                    {
                        // good to go, best case
                    }
                    else if ( outputLocation.isFile() )
                    {
                        // Use parent directory, warn user
                        outputDirectory = outputDirectory.getParent();
                        LOGGER.warn( "Using parent directory {} for CSV output "
                                     + "because there will be a file for each "
                                     + "feature.",
                                     outputDirectory.toString() );
                    }
                    else
                    {
                        // If we have neither a file nor a directory, is issue.
                        String message = "Destination path " + d.getPath()
                                         + " needs to be changed to a directory"
                                         + " that can be written to.";
                        throw new ProjectConfigException( d, message );
                    }
                }

                SortedMap<Integer, StringJoiner> rows = new TreeMap<>();

                MetricOutputMultiMapByLeadThreshold<ScalarOutput> scalarOutput =
                        storedMetricOutput.getScalarOutput();

                if ( scalarOutput != null )
                {
                    SortedMap<Integer, StringJoiner> scalars =
                            CommaSeparated.getScalarRows( scalarOutput );
                    for ( Map.Entry<Integer, StringJoiner> e : scalars.entrySet() )
                    {
                        rows.put( e.getKey(), e.getValue() );
                    }
                }

                Path outputPath = Paths.get( outputDirectory + "/"
                                             + feature.getLocation().getLid()
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

    private static SortedMap<Integer, StringJoiner> getScalarRows(
            MetricOutputMultiMapByLeadThreshold<ScalarOutput> output )
    {
        SortedMap<Integer, StringJoiner> rows = new TreeMap<>();
        StringJoiner headerRow = new StringJoiner( "," );

        headerRow.add( "LEAD_TIME" );
        for ( Map.Entry<MapBiKey<MetricConstants, MetricConstants>,
                MetricOutputMapByLeadThreshold<ScalarOutput>> m
                : output.entrySet() )
        {
            String name = m.getKey().getFirstKey().name();

            for ( Threshold t : m.getValue().keySetByThreshold() )
            {
                String column = name + "_" + t;
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

        SortedMap<Integer,StringJoiner> result = new TreeMap<>();
        result.put(Integer.MIN_VALUE, headerRow);

        for ( Map.Entry<Integer, StringJoiner> e : rows.entrySet() )
        {
            result.put( e.getKey(), e.getValue() );
        }

        return result;
    }
}
