package wres.vis.writing;

import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jfree.chart.JFreeChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.TimeWindow;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;
import wres.vis.charts.GraphicsUtils;

/**
 * Helps write charts comprising {@link BoxplotStatisticOuter} to graphics formats.
 *
 * @author James Brown
 */

public class BoxplotGraphicsWriter extends GraphicsWriter
        implements Function<List<BoxplotStatisticOuter>, Set<Path>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( BoxplotGraphicsWriter.class );

    private static final String SPECIFY_NON_NULL_INPUT_DATA_WHEN_WRITING_DIAGRAM_OUTPUTS =
            "Specify non-null input data when writing box plot outputs.";

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
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
            // for each group (e.g., one path for each window with DatasetOrientation.RIGHT data and one for
            // each window with DatasetOrientation.BASELINE data): #48287
            Map<DatasetOrientation, List<BoxplotStatisticOuter>> groups =
                    Slicer.getGroupedStatistics( filtered );

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
            // for each group (e.g., one path for each window with DatasetOrientation.RIGHT data and one for
            // each window with DatasetOrientation.BASELINE data): #48287
            Map<DatasetOrientation, List<BoxplotStatisticOuter>> groups =
                    Slicer.getGroupedStatistics( filtered );

            for ( List<BoxplotStatisticOuter> nextGroup : groups.values() )
            {
                // Slice by ensemble averaging type
                List<List<BoxplotStatisticOuter>> sliced =
                        BoxplotGraphicsWriter.getSlicedBoxes( nextGroup );

                for ( List<BoxplotStatisticOuter> nextSlice : sliced )
                {
                    Set<Path> innerPathsWrittenTo =
                            BoxplotGraphicsWriter.writeOneBoxPlotChartPerMetricAndLeadDurationSequence( super.getOutputDirectory(),
                                                                                                        super.getOutputsDescription(),
                                                                                                        nextSlice );
                    paths.addAll( innerPathsWrittenTo );
                }
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
            MetricConstants metricName = statistics.get( 0 )
                                                   .getMetricName();
            PoolMetadata metadata = statistics.get( 0 )
                                              .getPoolMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                Map<TimeWindowOuter, JFreeChart> charts = chartFactory.getBoxplotChartPerPool( statistics,
                                                                                               helper.getDurationUnits() );

                LOGGER.debug( "Created {} box plot charts, one for each of these time windows: {}.",
                              charts.size(),
                              charts.keySet() );

                // Build the outputs
                for ( Map.Entry<TimeWindowOuter, JFreeChart> nextEntry : charts.entrySet() )
                {
                    TimeWindowOuter appendObject = nextEntry.getKey();
                    String appendString = BoxplotGraphicsWriter.getPathQualifier( appendObject, statistics, helper );
                    Path outputImage = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                              metadata,
                                                                              appendString,
                                                                              metricName,
                                                                              null );

                    JFreeChart chart = nextEntry.getValue();

                    // Write formats
                    Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                           chart,
                                                                           metricName.getCanonicalName(),
                                                                           nextOutput );

                    pathsWrittenTo.addAll( finishedPaths );
                }
            }
        }
        catch ( ChartBuildingException e )
        {
            throw new GraphicsWriteException( "Error while generating box plot charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Generates a path qualifier for diagram and box-plot-style graphics based on the statistics provided.
     *
     * @param appendObject the object to use in the path qualifier
     * @param statistics the statistics
     * @param helper the graphics helper
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( Object appendObject,
                                            List<BoxplotStatisticOuter> statistics,
                                            GraphicsHelper helper )
    {
        String append = "";

        if ( appendObject instanceof TimeWindowOuter timeWindow )
        {
            Outputs.GraphicFormat.GraphicShape shape = helper.getGraphicShape();
            ChronoUnit leadUnits = helper.getDurationUnits();

            // Qualify pooling windows with the latest reference time and valid time
            if ( shape == Outputs.GraphicFormat.GraphicShape.ISSUED_DATE_POOLS
                 || shape == Outputs.GraphicFormat.GraphicShape.VALID_DATE_POOLS )
            {
                append = DataUtilities.toStringSafe( timeWindow, leadUnits );
            }
            // Needs to be fully qualified, but this would change the file names, which is arguably a breaking change
            // See GitHub ticket #540
            else if ( !timeWindow.getLatestLeadDuration()
                                 .equals( TimeWindowOuter.DURATION_MAX ) )
            {
                append = DataUtilities.toStringSafe( timeWindow.getLatestLeadDuration(), leadUnits )
                         + "_"
                         + leadUnits.name()
                                    .toUpperCase();
            }
        }
        else if ( appendObject instanceof OneOrTwoThresholds threshold )
        {
            append = DataUtilities.toStringSafe( threshold );
        }
        else
        {
            throw new UnsupportedOperationException( "Unexpected situation where WRES could not create "
                                                     + "outputImage path" );
        }

        // Non-default averaging types that should be qualified?
        // #51670
        SortedSet<EnsembleAverageType> types =
                Slicer.discover( statistics,
                                 next -> next.getPoolMetadata()
                                             .getPoolDescription()
                                             .getEnsembleAverageType() );

        Optional<EnsembleAverageType> type =
                types.stream()
                     .filter( next -> next != EnsembleAverageType.MEAN
                                      && next != EnsembleAverageType.NONE
                                      && next != EnsembleAverageType.UNRECOGNIZED )
                     .findFirst();

        if ( type.isPresent() )
        {
            append += "_ENSEMBLE_" + type.get()
                                         .name();
        }

        return append;
    }

    /**
     * Writes a box plot chart for all {@link BoxplotStatisticOuter} associated with each pooling window, i.e., all 
     * lead duration statistics for one valid time or issued time pool. If there is a single pool, creates one plot.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription the outputs
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeOneBoxPlotChartPerMetricAndLeadDurationSequence( Path outputDirectory,
                                                                                   Outputs outputsDescription,
                                                                                   List<BoxplotStatisticOuter> statistics )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory factory = GraphicsWriter.getChartFactory();

        // Build chart
        try
        {
            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                // Slice the statistics by lead duration sequence
                Map<TimeWindowOuter, List<BoxplotStatisticOuter>> sliced =
                        BoxplotGraphicsWriter.getSliceByPoolingWindow( statistics );

                for ( Map.Entry<TimeWindowOuter, List<BoxplotStatisticOuter>> nextPlot : sliced.entrySet() )
                {
                    TimeWindowOuter nextTimeWindow = nextPlot.getKey();
                    List<BoxplotStatisticOuter> nextStatistics = nextPlot.getValue();
                    MetricConstants metricName = nextStatistics.get( 0 )
                                                               .getMetricName();
                    PoolMetadata metadata = nextStatistics.get( 0 )
                                                          .getPoolMetadata();

                    // Build the chart engine
                    JFreeChart chart = factory.getBoxplotChart( nextStatistics,
                                                                helper.getDurationUnits() );

                    String append = BoxplotGraphicsWriter.getPathQualifier( statistics, nextTimeWindow, sliced.size() );

                    Path outputImage = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                              metadata,
                                                                              append,
                                                                              metricName,
                                                                              null );

                    // Write formats
                    Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                           chart,
                                                                           metricName.getCanonicalName(),
                                                                           nextOutput );

                    pathsWrittenTo.addAll( finishedPaths );
                }
            }
        }
        catch ( ChartBuildingException e )
        {
            throw new GraphicsWriteException( "Error while generating box plot charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Slices the statistics for individual graphics. Returns as many slices as graphics to create.
     *
     * @param statistics the statistics to slice
     * @return the sliced statistics to write
     */

    private static List<List<BoxplotStatisticOuter>> getSlicedBoxes( List<BoxplotStatisticOuter> statistics )
    {
        List<List<BoxplotStatisticOuter>> sliced = new ArrayList<>();

        // Slice by ensemble averaging function
        for ( EnsembleAverageType type : EnsembleAverageType.values() )
        {
            List<BoxplotStatisticOuter> innerSlice = Slicer.filter( statistics,
                                                                    value -> type == value.getPoolMetadata()
                                                                                          .getPoolDescription()
                                                                                          .getEnsembleAverageType() );

            if ( !innerSlice.isEmpty() )
            {
                // Group by threshold
                Map<ThresholdOuter, List<BoxplotStatisticOuter>> groupedByThreshold =
                        innerSlice.stream()
                                  .collect( Collectors.groupingBy( c -> c.getPoolMetadata()
                                                                         .getThresholds()
                                                                         .first() ) );
                for ( Map.Entry<ThresholdOuter, List<BoxplotStatisticOuter>> boxes : groupedByThreshold.entrySet() )
                {
                    List<BoxplotStatisticOuter> nextSlice = boxes.getValue();

                    // Group by summary statistic presence/absence
                    List<List<BoxplotStatisticOuter>> grouped =
                            GraphicsWriter.groupBySummaryStatistics( nextSlice,
                                                                     s -> s.getStatistic()
                                                                           .getMetric()
                                                                           .getStatisticName()
                                                                          + "_"
                                                                          + s.getStatistic()
                                                                             .getMetric()
                                                                             .getStatisticComponentName(),
                                                                     Set.of( SummaryStatistic.StatisticName.BOX_PLOT ) );
                    sliced.addAll( grouped );
                }
            }
        }

        return Collections.unmodifiableList( sliced );
    }

    /**
     * Slices the statistics for individual graphics by time-based pooling window. Returns as many slices as graphics 
     * to create.
     *
     * @param statistics the statistics to slice
     * @return the sliced statistics to write
     */

    private static Map<TimeWindowOuter, List<BoxplotStatisticOuter>> getSliceByPoolingWindow( List<BoxplotStatisticOuter> statistics )
    {
        Function<BoxplotStatisticOuter, TimeWindowOuter> classifier = boxplot -> {
            TimeWindowOuter timeWindow = boxplot.getPoolMetadata()
                                                .getTimeWindow();

            // Create a time window without lead duration qualifiers
            TimeWindow adjusted = timeWindow.getTimeWindow()
                                            .toBuilder()
                                            .setEarliestLeadDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                  .setSeconds(
                                                                                                          TimeWindowOuter.DURATION_MIN.getSeconds() )
                                                                                                  .setNanos(
                                                                                                          TimeWindowOuter.DURATION_MIN.getNano() ) )
                                            .setLatestLeadDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                .setSeconds(
                                                                                                        TimeWindowOuter.DURATION_MAX.getSeconds() )
                                                                                                .setNanos(
                                                                                                        TimeWindowOuter.DURATION_MAX.getNano() ) )
                                            .build();

            return TimeWindowOuter.of( adjusted );
        };

        return statistics.stream()
                         .collect( Collectors.groupingBy( classifier ) );
    }

    /**
     * Generates a path qualifier for the graphic based on the statistics provided.
     * @param statistics the statistics
     * @param timeWindow the time window
     * @param sliceCount the number of plots/slices produced
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( List<BoxplotStatisticOuter> statistics,
                                            TimeWindowOuter timeWindow,
                                            int sliceCount )
    {
        String append = "";

        // Qualify the time-based pools in the path if there is more than one plot
        if ( sliceCount > 1 )
        {
            append = DataUtilities.toStringSafe( timeWindow.getEarliestReferenceTime() )
                     + "_TO_"
                     + DataUtilities.toStringSafe( timeWindow.getLatestReferenceTime() )
                     + "_"
                     + DataUtilities.toStringSafe( timeWindow.getEarliestValidTime() )
                     + "_TO_"
                     + DataUtilities.toStringSafe( timeWindow.getLatestValidTime() );
        }

        // Non-default averaging types that should be qualified?
        // #51670
        SortedSet<EnsembleAverageType> types =
                Slicer.discover( statistics,
                                 next -> next.getPoolMetadata()
                                             .getPoolDescription()
                                             .getEnsembleAverageType() );

        Optional<EnsembleAverageType> type =
                types.stream()
                     .filter( next -> next != EnsembleAverageType.MEAN
                                      && next != EnsembleAverageType.NONE
                                      && next != EnsembleAverageType.UNRECOGNIZED )
                     .findFirst();

        if ( type.isPresent() )
        {
            if ( !append.isBlank() )
            {
                append += "_";
            }

            append += "ENSEMBLE_" + type.get()
                                        .name();
        }

        // Qualify by summary statistic name
        String name = BoxplotGraphicsWriter.getSummaryStatisticPathQualifier( statistics );

        if ( !name.isBlank() )
        {
            if ( !append.isBlank() )
            {
                append += "_";
            }
            append += name;
        }

        // Qualify by threshold, if not "all data"
        Optional<ThresholdOuter> matched = statistics.stream()
                                                     .map( t -> t.getPoolMetadata()
                                                                 .getThresholds()
                                                                 .first() )
                                                     .filter( t -> !t.isAllDataThreshold() )
                                                     .findFirst();

        if ( matched.isPresent() )
        {
            if ( !append.isBlank() )
            {
                append += "_";
            }
            append += DataUtilities.toStringSafe( matched.get() );
        }

        return append;
    }

    /**
     * Generates a path qualifier for summary statistics.
     * @param statistics the statistics
     * @return the path qualifier
     */
    private static String getSummaryStatisticPathQualifier( List<BoxplotStatisticOuter> statistics )
    {
        Optional<String> name = Optional.empty();
        Optional<String> componentName = Optional.empty();

        if ( statistics.stream()
                       .anyMatch( n -> n.isSummaryStatistic()
                                       && n.getSummaryStatistic()
                                           .getStatistic()
                                          == SummaryStatistic.StatisticName.BOX_PLOT ) )
        {
            List<BoxplotMetric> metrics = statistics.stream()
                                                    .filter( n -> n.isSummaryStatistic()
                                                                  && !n.getSummaryStatistic()
                                                                       .getDimensionList()
                                                                       .contains( SummaryStatistic.StatisticDimension.RESAMPLED ) )
                                                    .map( d -> d.getStatistic()
                                                                .getMetric() )
                                                    .toList();
            name = metrics.stream()
                          .map( m -> m.getStatisticName()
                                      .toString() )
                          .findFirst();
            componentName = metrics.stream()
                                   .map( BoxplotMetric::getStatisticComponentName )
                                   .filter( GraphicsUtils::isNotDefaultMetricComponentName )
                                   .map( Enum::name )
                                   .findFirst();
        }

        String combined = name.orElse( "" );
        if ( componentName.isPresent() )
        {
            combined += "_" + componentName.get();
        }

        return combined;
    }

    /**
     * Hidden constructor.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null
     */

    private BoxplotGraphicsWriter( Outputs outputsDescription,
                                   Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
