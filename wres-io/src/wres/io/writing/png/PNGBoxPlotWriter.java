package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link BoxPlotStatistics} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGBoxPlotWriter extends PNGWriter
        implements Consumer<ListOfStatistics<BoxPlotStatistics>>,
        Supplier<Set<Path>>
{
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

    public static PNGBoxPlotWriter of( ProjectConfigPlus projectConfigPlus,
                                       ChronoUnit durationUnits,
                                       Path outputDirectory )
    {
        return new PNGBoxPlotWriter( projectConfigPlus, durationUnits, outputDirectory );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfStatistics<BoxPlotStatistics> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( super.getProjectConfigPlus().getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {


            // Iterate through types per pair
            ListOfStatistics<BoxPlotStatistics> perPair =
                    Slicer.filter( output, meta -> meta.getMetricID().isInGroup( StatisticGroup.BOXPLOT_PER_PAIR ) );

            SortedSet<MetricConstants> metricsPerPair =
                    Slicer.discover( perPair, meta -> meta.getMetadata().getMetricID() );
            for ( MetricConstants next : metricsPerPair )
            {
                Set<Path> innerPathsWrittenTo =
                        PNGBoxPlotWriter.writeOneBoxPlotChartPerMetricAndPool( super.getOutputDirectory(),
                                                                               super.getProjectConfigPlus(),
                                                                               destinationConfig,
                                                                               Slicer.filter( perPair, next ),
                                                                               super.getDurationUnits() );
                this.pathsWrittenTo.addAll( innerPathsWrittenTo );
            }

            // Iterate through the pool types
            ListOfStatistics<BoxPlotStatistics> perPool =
                    Slicer.filter( output, meta -> meta.getMetricID().isInGroup( StatisticGroup.BOXPLOT_PER_POOL ) );

            SortedSet<MetricConstants> metricsPerPool =
                    Slicer.discover( perPool, meta -> meta.getMetadata().getMetricID() );
            for ( MetricConstants next : metricsPerPool )
            {
                Set<Path> innerPathsWrittenTo =
                        PNGBoxPlotWriter.writeOneBoxPlotChartPerMetric( super.getOutputDirectory(),
                                                                        super.getProjectConfigPlus(),
                                                                        destinationConfig,
                                                                        Slicer.filter( perPool, next ),
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
     * Writes a separate box plot chart for each {@link BoxPlotStatistics} in the {@link ListOfStatistics}
     * provided. Each {@link BoxPlotStatistics} represents one metric result for one pool or 
     * {@link TimeWindow}.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results, which contains all results for one metric across several pools
     * @param durationUnits the time units for durations
     * @throws PNGWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeOneBoxPlotChartPerMetricAndPool( Path outputDirectory,
                                                                   ProjectConfigPlus projectConfigPlus,
                                                                   DestinationConfig destinationConfig,
                                                                   ListOfStatistics<BoxPlotStatistics> output,
                                                                   ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            StatisticMetadata meta = output.getData().get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            Map<Pair<TimeWindow, OneOrTwoThresholds>, ChartEngine> engines =
                    ChartEngineFactory.buildBoxPlotChartEnginePerPool( projectConfigPlus.getProjectConfig(),
                                                                       output,
                                                                       helper.getTemplateResourceName(),
                                                                       helper.getGraphicsString(),
                                                                       durationUnits );

            // Build the outputs
            for ( final Entry<Pair<TimeWindow, OneOrTwoThresholds>, ChartEngine> nextEntry : engines.entrySet() )
            {
                Path outputImage = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                      destinationConfig,
                                                                      meta,
                                                                      nextEntry.getKey().getLeft(),
                                                                      durationUnits );

                PNGWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
                // Only if writeChart succeeded do we assume that it was written
                pathsWrittenTo.add( outputImage );
            }
        }
        catch ( ChartEngineException | IOException e )
        {
            throw new PNGWriteException( "Error while generating box plot charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Writes a single box plot chart for all {@link BoxPlotStatistics} in the {@link ListOfStatistics}
     * provided.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results, which contains all results for one metric across several pools
     * @param durationUnits the time units for durations
     * @throws PNGWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeOneBoxPlotChartPerMetric( Path outputDirectory,
                                                            ProjectConfigPlus projectConfigPlus,
                                                            DestinationConfig destinationConfig,
                                                            ListOfStatistics<BoxPlotStatistics> output,
                                                            ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build chart
        try
        {
            StatisticMetadata metadata = output.getData().get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, metadata.getMetricID() );

            // Pool the output
            List<BoxPlotStatistic> pool = new ArrayList<>();
            output.forEach( next -> pool.addAll( next.getData() ) );
            BoxPlotStatistics pooled = BoxPlotStatistics.of( pool, metadata );

            // Build the chart engine
            ChartEngine engine =
                    ChartEngineFactory.buildBoxPlotChartEngine( projectConfigPlus.getProjectConfig(),
                                                                pooled,
                                                                helper.getTemplateResourceName(),
                                                                helper.getGraphicsString(),
                                                                durationUnits );

            Path outputImage = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                  destinationConfig,
                                                                  metadata );

            PNGWriter.writeChart( outputImage, engine, destinationConfig );

            // Only if writeChart succeeded do we assume that it was written
            pathsWrittenTo.add( outputImage );

        }
        catch ( ChartEngineException | IOException e )
        {
            throw new PNGWriteException( "Error while generating box plot charts: ", e );
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

    private PNGBoxPlotWriter( ProjectConfigPlus projectConfigPlus,
                              ChronoUnit durationUnits,
                              Path outputDirectory )
    {
        super( projectConfigPlus, durationUnits, outputDirectory );
    }

}
