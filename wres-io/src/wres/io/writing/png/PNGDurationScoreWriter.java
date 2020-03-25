package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.StatisticMetadata;
import wres.io.config.ConfigHelper;
import wres.system.SystemSettings;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DurationScoreStatistic} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGDurationScoreWriter extends PNGWriter
        implements Consumer<ListOfStatistics<DurationScoreStatistic>>,
        Supplier<Set<Path>>
{
    private Set<Path> pathsWrittenTo = new HashSet<>();

    /**
     * Returns an instance of a writer.
     *
     * @param systemSettings The system settings to use.
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGDurationScoreWriter of( SystemSettings systemSettings,
                                             ProjectConfigPlus projectConfigPlus,
                                             ChronoUnit durationUnits,
                                             Path outputDirectory )
    {
        return new PNGDurationScoreWriter( systemSettings,
                                           projectConfigPlus,
                                           durationUnits,
                                           outputDirectory );
    }

    /**
     * Writes all output for one score type.
     *
     * @param output the score output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfStatistics<DurationScoreStatistic> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( super.getProjectConfigPlus().getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {
            // Iterate through each metric 
            SortedSet<MetricConstants> metrics = Slicer.discover( output, meta -> meta.getMetadata().getMetricID() );
            for ( MetricConstants next : metrics )
            {
                Set<Path> innerPathsWrittenTo =
                        PNGDurationScoreWriter.writeScoreCharts( super.getSystemSettings(),
                                                                 super.getOutputDirectory(),
                                                                 super.getProjectConfigPlus(),
                                                                 destinationConfig,
                                                                 Slicer.filter( output, next ),
                                                                 super.getDurationUnits() );
                this.pathsWrittenTo.addAll( innerPathsWrittenTo );
            }
        }
    }

    /**
     * Return a snapshot of the paths written to (so far)
     * 
     * @return the paths written so far.
     */

    @Override
    public Set<Path> get()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    /**
     * Writes a set of charts associated with {@link DurationScoreStatistic} for a single metric and time window,
     * stored in a {@link ListOfStatistics}.
     *
     * @param systemSettings The system settings to use.
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric output
     * @param durationUnits the time units for durations
     * @throws PNGWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeScoreCharts( SystemSettings systemSettings,
                                               Path outputDirectory,
                                               ProjectConfigPlus projectConfigPlus,
                                               DestinationConfig destinationConfig,
                                               ListOfStatistics<DurationScoreStatistic> output,
                                               ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            StatisticMetadata meta = output.getData().get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            ChartEngine engine =
                    ChartEngineFactory.buildCategoricalDurationScoreChartEngine( projectConfigPlus.getProjectConfig(),
                                                                                 output,
                                                                                 helper.getTemplateResourceName(),
                                                                                 helper.getGraphicsString(),
                                                                                 durationUnits );


            // Build the output file name
            Path outputImage = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                  destinationConfig,
                                                                  meta );

            PNGWriter.writeChart( systemSettings, outputImage, engine, destinationConfig );
            // Only if writeChart succeeded do we assume that it was written
            pathsWrittenTo.add( outputImage );
        }
        catch ( ChartEngineException | IOException e )
        {
            throw new PNGWriteException( "Error while generating multi-vector charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private PNGDurationScoreWriter( SystemSettings systemSettings,
                                    ProjectConfigPlus projectConfigPlus,
                                    ChronoUnit durationUnits,
                                    Path outputDirectory )
    {
        super( systemSettings, projectConfigPlus, durationUnits, outputDirectory );
    }

}
