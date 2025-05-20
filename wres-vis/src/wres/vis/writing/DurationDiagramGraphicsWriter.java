package wres.vis.writing;

import java.nio.file.Path;
import java.util.Arrays;
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

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.SummaryStatistic;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;

/**
 * Helps write charts comprising {@link DurationDiagramStatisticOuter} to graphics formats.
 *
 * @author James Brown
 */

public class DurationDiagramGraphicsWriter extends GraphicsWriter
        implements Function<List<DurationDiagramStatisticOuter>, Set<Path>>
{

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     */

    public static DurationDiagramGraphicsWriter of( Outputs outputsDescription,
                                                    Path outputDirectory )
    {
        return new DurationDiagramGraphicsWriter( outputsDescription, outputDirectory );
    }

    /**
     * Writes all output for one paired type.
     *
     * @param output the paired output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<DurationDiagramStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing duration diagram outputs." );

        Set<Path> paths = new HashSet<>();

        // Iterate through each metric 
        SortedSet<MetricConstants> metrics =
                Slicer.discover( output, DurationDiagramStatisticOuter::getMetricName );
        for ( MetricConstants next : metrics )
        {
            List<DurationDiagramStatisticOuter> filtered = Slicer.filter( output, next );

            // Group the statistics by the LRB context in which they appear. There will be one path written
            // for each group (e.g., one path for each window with DatasetOrientation.RIGHT data and one for
            // each window with DatasetOrientation.BASELINE data): #48287
            Map<DatasetOrientation, List<DurationDiagramStatisticOuter>> groups =
                    Slicer.getGroupedStatistics( filtered );

            for ( List<DurationDiagramStatisticOuter> nextGroup : groups.values() )
            {
                Set<Path> innerPathsWrittenTo =
                        DurationDiagramGraphicsWriter.writePairedOutputByInstantDurationCharts( super.getOutputDirectory(),
                                                                                                super.getOutputsDescription(),
                                                                                                nextGroup );
                paths.addAll( innerPathsWrittenTo );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a set of charts associated with {@link DurationDiagramStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the outputs required
     * @param statistics the metric results
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writePairedOutputByInstantDurationCharts( Path outputDirectory,
                                                                       Outputs outputsDescription,
                                                                       List<DurationDiagramStatisticOuter> statistics )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory chartFactory = GraphicsWriter.getChartFactory();

        // Build charts
        try
        {
            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            // Group by summary statistic presence/absence
            // Qualify by quantile where required
            Function<DurationDiagramStatisticOuter, String> qualifier = d ->
            {
                if ( d.isSummaryStatistic()
                     && d.getSummaryStatistic()
                         .getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
                {
                    return Double.toString( d.getSummaryStatistic().
                                             getProbability() )
                                 .replace( ".", "_" );
                }
                return "";
            };

            List<List<DurationDiagramStatisticOuter>> grouped
                    = GraphicsWriter.groupBySummaryStatistics( statistics,
                                                               qualifier,
                                                               Arrays.stream( SummaryStatistic.StatisticName.values() )
                                                                     .collect( Collectors.toUnmodifiableSet() ) );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                for ( List<DurationDiagramStatisticOuter> nextStatistics : grouped )
                {
                    List<DurationDiagramStatisticOuter> filtered =
                            DurationDiagramGraphicsWriter.filterEmptyStatistics( nextStatistics );

                    if ( !filtered.isEmpty() )
                    {
                        DurationDiagramStatisticOuter next = filtered.get( 0 );

                        MetricConstants metricName = next.getMetricName();
                        PoolMetadata metadata = GraphicsWriter.getPoolMetadata( filtered );
                        String pathQualifier = DurationDiagramGraphicsWriter.getPathQualifier( filtered );

                        JFreeChart chart = chartFactory.getDurationDiagramChart( filtered,
                                                                                 helper.getDurationUnits() );

                        // Build the output file name
                        Path outputImage = DataUtilities.getPathFromPoolMetadata( outputDirectory,
                                                                                  metadata,
                                                                                  pathQualifier,
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
        }
        catch ( ChartBuildingException e )
        {
            throw new GraphicsWriteException( "Error while generating duration diagram charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Removes any statistics duration diagrams that do not contain any statistics.
     *
     * @param diagrams the diagrams
     * @return the filtered diagrams
     */

    private static List<DurationDiagramStatisticOuter> filterEmptyStatistics( List<DurationDiagramStatisticOuter> diagrams )
    {
        return diagrams.stream().filter( d -> !d.getStatistic()
                                                .getStatisticsList()
                                                .isEmpty() )
                       .toList();
    }

    /**
     * Generates a path qualifier for the graphic based on the statistics provided.
     * @param statistics the statistics
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( List<DurationDiagramStatisticOuter> statistics )
    {
        String append = "";

        // Qualify by summary statistic
        Optional<SummaryStatistic> statistic =
                statistics.stream()
                          .filter( n -> n.isSummaryStatistic()
                                        && !n.getSummaryStatistic()
                                             .getDimensionList()
                                             .contains( SummaryStatistic.StatisticDimension.RESAMPLED ) )
                          .map( DurationDiagramStatisticOuter::getSummaryStatistic )
                          .findFirst();

        if ( statistic.isPresent() )
        {
            SummaryStatistic use = statistic.get();
            append = use.getStatistic()
                        .toString();
            if ( use.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
            {
                append += "_"
                          + Double.toString( use.getProbability() )
                                  .replace( ".", "_" );
            }
        }

        return append;
    }

    /**
     * Hidden constructor.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null
     */

    private DurationDiagramGraphicsWriter( Outputs outputsDescription,
                                           Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
