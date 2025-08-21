package wres.vis.writing;

import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.Predicate;

import org.jfree.chart.JFreeChart;

import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Pool.EnsembleAverageType;
import wres.statistics.generated.SummaryStatistic;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;
import wres.vis.charts.GraphicsUtils;

/**
 * Helps write charts comprising {@link DiagramStatisticOuter} to graphics formats.
 *
 * @author James Brown
 */

public class DiagramGraphicsWriter extends GraphicsWriter
        implements Function<List<DiagramStatisticOuter>, Set<Path>>
{

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     */

    public static DiagramGraphicsWriter of( Outputs outputsDescription,
                                            Path outputDirectory )
    {
        return new DiagramGraphicsWriter( outputsDescription, outputDirectory );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param output the diagram output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<DiagramStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        Set<Path> paths = new HashSet<>();

        // Iterate through each metric 
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DiagramStatisticOuter::getMetricName );
        for ( MetricConstants next : metrics )
        {
            List<DiagramStatisticOuter> filtered = Slicer.filter( output, next );

            // Slice the statistics
            List<List<DiagramStatisticOuter>> sliced =
                    DiagramGraphicsWriter.getSlicedStatistics( filtered );

            for ( List<DiagramStatisticOuter> nextSlice : sliced )
            {
                Set<Path> innerPathsWrittenTo =
                        DiagramGraphicsWriter.writeDiagrams( super.getOutputDirectory(),
                                                             super.getOutputsDescription(),
                                                             nextSlice );
                paths.addAll( innerPathsWrittenTo );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a set of charts associated with {@link DiagramStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the outputs required
     * @param statistics the metric results
     * @return the paths written
     * @throws GraphicsWriteException when an error occurs during writing
     */

    private static Set<Path> writeDiagrams( Path outputDirectory,
                                            Outputs outputsDescription,
                                            List<DiagramStatisticOuter> statistics )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory chartFactory = GraphicsWriter.getChartFactory();

        // Build charts
        try
        {
            MetricConstants metricName = statistics.get( 0 )
                                                   .getMetricName();
            PoolMetadata metadata = GraphicsWriter.getPoolMetadata( statistics );

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                Map<Object, JFreeChart> engines = chartFactory.getDiagramCharts( statistics,
                                                                                 helper.getGraphicShape(),
                                                                                 helper.getDurationUnits() );

                // Build the outputs
                for ( Entry<Object, JFreeChart> nextEntry : engines.entrySet() )
                {
                    // Build the output file name
                    Object appendObject = nextEntry.getKey();
                    String appendString = DiagramGraphicsWriter.getPathQualifier( appendObject, statistics, helper );
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
            throw new GraphicsWriteException( "Error while generating diagram charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Slices the statistics for individual graphics. Returns as many sliced lists of statistics as graphics to create.
     *
     * @param statistics the statistics to slice
     * @return the sliced statistics to write
     */

    private static List<List<DiagramStatisticOuter>> getSlicedStatistics( List<DiagramStatisticOuter> statistics )
    {
        List<List<DiagramStatisticOuter>> sliced = new ArrayList<>();

        // Slice by ensemble averaging function
        for ( EnsembleAverageType type : EnsembleAverageType.values() )
        {
            List<DiagramStatisticOuter> innerSlice = Slicer.filter( statistics,
                                                                    value -> type == value.getPoolMetadata()
                                                                                          .getPoolDescription()
                                                                                          .getEnsembleAverageType() );
            if ( !innerSlice.isEmpty() )
            {
                // Group by summary statistic presence/absence, only allowing non-summary statistic diagrams when a
                // median is present

                // Remove quantile statistics when no median is present
                innerSlice = DiagramGraphicsWriter.removeSummaryStatisticQuantilesWithoutMedian( innerSlice );

                List<List<DiagramStatisticOuter>> grouped =
                        GraphicsWriter.groupBySummaryStatistics( innerSlice,
                                                                 s -> s.getStatistic().getMetric()
                                                                       .getStatisticName()
                                                                      + "_"
                                                                      + s.getStatistic().getMetric()
                                                                         .getStatisticComponentName(),
                                                                 Set.of( SummaryStatistic.StatisticName.MEAN,
                                                                         SummaryStatistic.StatisticName.MEDIAN,
                                                                         SummaryStatistic.StatisticName.HISTOGRAM,
                                                                         SummaryStatistic.StatisticName.QUANTILE ) );
                sliced.addAll( grouped );
            }
        }

        return Collections.unmodifiableList( sliced );
    }

    /**
     * Filters the supplied statistics, removing any summary statistic quantiles that do not have a median present.
     *
     * @param diagrams the statistics to filter
     * @return the filtered statistics
     */

    private static List<DiagramStatisticOuter> removeSummaryStatisticQuantilesWithoutMedian
    ( List<DiagramStatisticOuter> diagrams )
    {
        List<DiagramStatisticOuter> filtered = diagrams;
        Predicate<DiagramStatisticOuter> filter = s -> s.isSummaryStatistic()
                                                       && s.getSummaryStatistic()
                                                           .getStatistic()
                                                          == SummaryStatistic.StatisticName.QUANTILE
                                                       && !s.getSummaryStatistic()
                                                            .getDimensionList()
                                                            .contains( SummaryStatistic.StatisticDimension.RESAMPLED );
        if ( diagrams.stream()
                     .anyMatch( filter )
             && diagrams.stream()
                        .noneMatch( s -> s.isSummaryStatistic()
                                         && s.getSummaryStatistic()
                                             .getProbability() == 0.5 ) )
        {
            filtered = diagrams.stream()
                               .filter( filter.negate() )
                               .toList();
        }

        return filtered;
    }

    /**
     * Generates a path qualifier for the graphic based on the statistics provided.
     * @param appendObject the object to use in the path qualifier
     * @param statistics the statistics
     * @param helper the graphics helper
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( Object appendObject,
                                            List<DiagramStatisticOuter> statistics,
                                            GraphicsHelper helper )
    {
        String append;

        if ( appendObject instanceof TimeWindowOuter timeWindow )
        {
            GraphicShape shape = helper.getGraphicShape();
            ChronoUnit leadUnits = helper.getDurationUnits();

            append = GraphicsWriter.getPathQualifier( timeWindow, shape, leadUnits );
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

        // Qualify by summary statistic name
        String name = DiagramGraphicsWriter.getSummaryStatisticPathQualifier( statistics );

        if ( !name.isBlank() )
        {
            append += "_" + name;
        }

        return append;
    }

    /**
     * Generates a path qualifier for summary statistics.
     * @param statistics the statistics
     * @return the path qualifier
     */
    private static String getSummaryStatisticPathQualifier( List<DiagramStatisticOuter> statistics )
    {
        Optional<String> name;
        Optional<String> componentName = Optional.empty();

        if ( statistics.stream()
                       .anyMatch( n -> n.isSummaryStatistic()
                                       && n.getSummaryStatistic().getStatistic()
                                          == SummaryStatistic.StatisticName.HISTOGRAM ) )
        {
            List<DiagramMetric> metrics = statistics.stream()
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
                                   .map( DiagramMetric::getStatisticComponentName )
                                   .filter( GraphicsUtils::isNotDefaultMetricComponentName )
                                   .map( Enum::name )
                                   .findFirst();
        }
        else
        {
            name = statistics.stream()
                             .filter( n -> n.isSummaryStatistic()
                                           && !n.getSummaryStatistic()
                                                .getDimensionList()
                                                .contains( SummaryStatistic.StatisticDimension.RESAMPLED ) )
                             .map( d -> d.getSummaryStatistic()
                                         .getStatistic()
                                         .name() )
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
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the required outputs
     * @throws NullPointerException if either input is null
     */

    private DiagramGraphicsWriter( Outputs outputsDescription,
                                   Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
