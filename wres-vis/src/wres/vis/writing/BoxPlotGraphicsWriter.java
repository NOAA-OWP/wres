package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.DestinationConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link BoxplotStatisticOuter} to graphics formats.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BoxPlotGraphicsWriter extends GraphicsWriter
        implements Consumer<List<BoxplotStatisticOuter>>,
        Supplier<Set<Path>>
{
    private static final String SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS =
            "Specify non-null input data when writing diagram outputs.";

    private Set<Path> pathsWrittenTo = new HashSet<>();

    /**
     * Returns an instance of a writer.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static BoxPlotGraphicsWriter of( ProjectConfigPlus projectConfigPlus,
                                            ChronoUnit durationUnits,
                                            Path outputDirectory )
    {
        return new BoxPlotGraphicsWriter( projectConfigPlus, durationUnits, outputDirectory );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public void accept( final List<BoxplotStatisticOuter> output )
    {
        Objects.requireNonNull( output, SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS );

        this.writeBoxPlotsPerPair( output );
        this.writeBoxPlotsPerPool( output );
    }

    /**
     * Writes all output for the {@link StatisticType#BOXPLOT_PER_PAIR}.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    private void writeBoxPlotsPerPair( List<BoxplotStatisticOuter> output )
    {
        Objects.requireNonNull( output, SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS );

        // Write output
        List<DestinationConfig> destinations =
                ProjectConfigs.getGraphicalDestinations( super.getProjectConfigPlus().getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {

            // Iterate through types per pair
            List<BoxplotStatisticOuter> perPair =
                    Slicer.filter( output, next -> next.getMetricName().isInGroup( StatisticType.BOXPLOT_PER_PAIR ) );

            SortedSet<MetricConstants> metricsPerPair =
                    Slicer.discover( perPair, BoxplotStatisticOuter::getMetricName );
            for ( MetricConstants next : metricsPerPair )
            {
                List<BoxplotStatisticOuter> filtered = Slicer.filter( perPair, next );

                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<LeftOrRightOrBaseline, List<BoxplotStatisticOuter>> groups =
                        Slicer.getStatisticsGroupedByContext( filtered );

                for ( List<BoxplotStatisticOuter> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            BoxPlotGraphicsWriter.writeOneBoxPlotChartPerMetricAndPool( super.getOutputDirectory(),
                                                                                        super.getProjectConfigPlus(),
                                                                                        destinationConfig,
                                                                                        nextGroup,
                                                                                        super.getDurationUnits() );
                    this.pathsWrittenTo.addAll( innerPathsWrittenTo );
                }
            }
        }
    }

    /**
     * Writes all output for the {@link StatisticType#BOXPLOT_PER_POOL}.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    private void writeBoxPlotsPerPool( List<BoxplotStatisticOuter> output )
    {
        Objects.requireNonNull( output, SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS );

        // Write output
        List<DestinationConfig> destinations =
                ProjectConfigs.getGraphicalDestinations( super.getProjectConfigPlus().getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {

            // Iterate through the pool types
            List<BoxplotStatisticOuter> perPool =
                    Slicer.filter( output, next -> next.getMetricName().isInGroup( StatisticType.BOXPLOT_PER_POOL ) );

            SortedSet<MetricConstants> metricsPerPool =
                    Slicer.discover( perPool, BoxplotStatisticOuter::getMetricName );
            for ( MetricConstants next : metricsPerPool )
            {
                List<BoxplotStatisticOuter> filtered = Slicer.filter( perPool, next );

                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<LeftOrRightOrBaseline, List<BoxplotStatisticOuter>> groups =
                        Slicer.getStatisticsGroupedByContext( filtered );

                for ( List<BoxplotStatisticOuter> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            BoxPlotGraphicsWriter.writeOneBoxPlotChartPerMetric( super.getOutputDirectory(),
                                                                                 super.getProjectConfigPlus(),
                                                                                 destinationConfig,
                                                                                 nextGroup,
                                                                                 super.getDurationUnits() );
                    this.pathsWrittenTo.addAll( innerPathsWrittenTo );
                }
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
     * Writes a separate box plot chart for each {@link BoxplotStatisticOuter} in the {@link List}
     * provided. Each {@link BoxplotStatisticOuter} represents one metric result for one pool or 
     * {@link TimeWindowOuter}.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results, which contains all results for one metric across several pools
     * @param durationUnits the time units for durations
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeOneBoxPlotChartPerMetricAndPool( Path outputDirectory,
                                                                   ProjectConfigPlus projectConfigPlus,
                                                                   DestinationConfig destinationConfig,
                                                                   List<BoxplotStatisticOuter> output,
                                                                   ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            SampleMetadata metadata = output.get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, metricName );

            Map<Pair<TimeWindowOuter, OneOrTwoThresholds>, ChartEngine> engines =
                    ChartEngineFactory.buildBoxPlotChartEnginePerPool( projectConfigPlus.getProjectConfig(),
                                                                       output,
                                                                       helper.getTemplateResourceName(),
                                                                       helper.getGraphicsString(),
                                                                       durationUnits );

            // Build the outputs
            for ( final Entry<Pair<TimeWindowOuter, OneOrTwoThresholds>, ChartEngine> nextEntry : engines.entrySet() )
            {
                Path outputImage = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                      destinationConfig,
                                                                      metadata,
                                                                      nextEntry.getKey().getLeft(),
                                                                      durationUnits,
                                                                      metricName,
                                                                      null );

                GraphicsWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
                // Only if writeChart succeeded do we assume that it was written
                pathsWrittenTo.add( outputImage );
            }
        }
        catch ( ChartEngineException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating box plot charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes a single box plot chart for all {@link BoxplotStatisticOuter} in the {@link List}
     * provided.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results, which contains all results for one metric across several pools
     * @param durationUnits the time units for durations
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeOneBoxPlotChartPerMetric( Path outputDirectory,
                                                            ProjectConfigPlus projectConfigPlus,
                                                            DestinationConfig destinationConfig,
                                                            List<BoxplotStatisticOuter> output,
                                                            ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build chart
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            SampleMetadata metadata = output.get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, metricName );

            // Build the chart engine
            ChartEngine engine =
                    ChartEngineFactory.buildBoxPlotChartEngine( projectConfigPlus.getProjectConfig(),
                                                                output,
                                                                helper.getTemplateResourceName(),
                                                                helper.getGraphicsString(),
                                                                durationUnits );

            Path outputImage = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                  destinationConfig,
                                                                  metadata,
                                                                  metricName,
                                                                  null );

            GraphicsWriter.writeChart( outputImage, engine, destinationConfig );

            // Only if writeChart succeeded do we assume that it was written
            pathsWrittenTo.add( outputImage );

        }
        catch ( ChartEngineException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating box plot charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private BoxPlotGraphicsWriter( ProjectConfigPlus projectConfigPlus,
                                   ChronoUnit durationUnits,
                                   Path outputDirectory )
    {
        super( projectConfigPlus, durationUnits, outputDirectory );
    }

}
