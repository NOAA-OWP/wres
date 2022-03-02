package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import org.jfree.chart.JFreeChart;

import wres.config.ProjectConfigException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DataFactory;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Outputs;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;

/**
 * Helps write charts comprising {@link BoxplotStatisticOuter} to graphics formats.
 * 
 * @author James Brown
 */

public class BoxplotGraphicsWriter extends GraphicsWriter
        implements Function<List<BoxplotStatisticOuter>, Set<Path>>
{
    private static final String SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS =
            "Specify non-null input data when writing box plot outputs.";

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static BoxplotGraphicsWriter of( Outputs outputsDescription,
                                            Path outputDirectory )
    {
        return new BoxplotGraphicsWriter( outputsDescription, outputDirectory );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<BoxplotStatisticOuter> output )
    {
        Objects.requireNonNull( output, SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS );

        Set<Path> perPairPaths = this.writeBoxPlotsPerPair( output );
        Set<Path> perPoolPaths = this.writeBoxPlotsPerPool( output );

        Set<Path> paths = new HashSet<>( perPairPaths );
        paths.addAll( perPoolPaths );

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes all output for the {@link StatisticType#BOXPLOT_PER_PAIR}.
     *
     * @param output the box plot output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    private Set<Path> writeBoxPlotsPerPair( List<BoxplotStatisticOuter> output )
    {
        Objects.requireNonNull( output, SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS );

        Set<Path> paths = new HashSet<>();

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
                        BoxplotGraphicsWriter.writeOneBoxPlotChartPerMetricAndPool( super.getOutputDirectory(),
                                                                                    super.getOutputsDescription(),
                                                                                    nextGroup );
                paths.addAll( innerPathsWrittenTo );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes all output for the {@link StatisticType#BOXPLOT_PER_POOL}.
     *
     * @param output the box plot output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    private Set<Path> writeBoxPlotsPerPool( List<BoxplotStatisticOuter> output )
    {
        Objects.requireNonNull( output, SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS );

        Set<Path> paths = new HashSet<>();

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
                        BoxplotGraphicsWriter.writeOneBoxPlotChartPerMetric( super.getOutputDirectory(),
                                                                             super.getOutputsDescription(),
                                                                             nextGroup );
                paths.addAll( innerPathsWrittenTo );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a separate box plot chart for each {@link BoxplotStatisticOuter} in the {@link List}
     * provided. Each {@link BoxplotStatisticOuter} represents one metric result for one pool or 
     * {@link TimeWindowOuter}.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the outputs required
     * @param statistics the metric results, which contains all results for one metric across several pools
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeOneBoxPlotChartPerMetricAndPool( Path outputDirectory,
                                                                   Outputs outputsDescription,
                                                                   List<BoxplotStatisticOuter> statistics )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory chartFactory = GraphicsWriter.getChartFactory();

        // Build charts
        try
        {
            MetricConstants metricName = statistics.get( 0 ).getMetricName();
            PoolMetadata metadata = statistics.get( 0 ).getMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                Map<TimeWindowOuter, JFreeChart> charts = chartFactory.getBoxplotChartPerPool( statistics,
                                                                                               helper.getDurationUnits() );

                // Build the outputs
                for ( Map.Entry<TimeWindowOuter, JFreeChart> nextEntry : charts.entrySet() )
                {
                    Path outputImage = DataFactory.getPathFromPoolMetadata( outputDirectory,
                                                                            metadata,
                                                                            nextEntry.getKey(),
                                                                            helper.getDurationUnits(),
                                                                            metricName,
                                                                            null );

                    JFreeChart chart = nextEntry.getValue();

                    // Write formats
                    Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                           chart,
                                                                           nextOutput );

                    pathsWrittenTo.addAll( finishedPaths );
                }
            }
        }
        catch ( ChartBuildingException | IOException e )
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
     * @param destinations the destinations for the written output
     * @param output the metric results, which contains all results for one metric across several pools
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeOneBoxPlotChartPerMetric( Path outputDirectory,
                                                            Outputs outputsDescription,
                                                            List<BoxplotStatisticOuter> output )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory factory = GraphicsWriter.getChartFactory();

        // Build chart
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            PoolMetadata metadata = output.get( 0 ).getMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                // Build the chart engine
                JFreeChart chart = factory.getBoxplotChart( output,
                                                            helper.getDurationUnits() );

                Path outputImage = DataFactory.getPathFromPoolMetadata( outputDirectory,
                                                                        metadata,
                                                                        metricName,
                                                                        null );

                // Write formats
                Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                       chart,
                                                                       nextOutput );

                pathsWrittenTo.addAll( finishedPaths );
            }
        }
        catch ( ChartBuildingException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating box plot charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     * 
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private BoxplotGraphicsWriter( Outputs outputsDescription,
                                   Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
