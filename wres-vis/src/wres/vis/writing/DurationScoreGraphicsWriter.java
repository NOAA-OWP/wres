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
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.SummaryStatistic;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;

/**
 * Helps write charts comprising {@link DurationScoreStatisticOuter} to graphics formats.
 *
 * @author James Brown
 */

public class DurationScoreGraphicsWriter extends GraphicsWriter
        implements Function<List<DurationScoreStatisticOuter>, Set<Path>>
{

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     */

    public static DurationScoreGraphicsWriter of( Outputs outputsDescription,
                                                  Path outputDirectory )
    {
        return new DurationScoreGraphicsWriter( outputsDescription,
                                                outputDirectory );
    }

    /**
     * Writes all output for one score type.
     *
     * @param output the score output
     * @return the paths written
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public Set<Path> apply( List<DurationScoreStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing duration score outputs." );

        Set<Path> paths = new HashSet<>();

        // Iterate through each metric 
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DurationScoreStatisticOuter::getMetricName );
        for ( MetricConstants next : metrics )
        {
            List<DurationScoreStatisticOuter> filtered = Slicer.filter( output, next );

            // Group the statistics by the LRB context in which they appear. There will be one path written
            // for each group (e.g., one path for each window with DatasetOrientation.RIGHT data and one for
            // each window with DatasetOrientation.BASELINE data): #48287
            Map<DatasetOrientation, List<DurationScoreStatisticOuter>> groups =
                    Slicer.getGroupedStatistics( filtered );

            for ( List<DurationScoreStatisticOuter> nextGroup : groups.values() )
            {
                Set<Path> innerPathsWrittenTo =
                        DurationScoreGraphicsWriter.writeScoreCharts( super.getOutputDirectory(),
                                                                      super.getOutputsDescription(),
                                                                      nextGroup );
                paths.addAll( innerPathsWrittenTo );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a set of charts associated with {@link DurationScoreStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the required outputs
     * @param statistics the metric output
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeScoreCharts( Path outputDirectory,
                                               Outputs outputsDescription,
                                               List<DurationScoreStatisticOuter> statistics )
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
            Function<DurationScoreStatisticOuter, String> qualifier = d ->
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

            List<List<DurationScoreStatisticOuter>> grouped
                    = GraphicsWriter.groupBySummaryStatistics( statistics,
                                                               qualifier,
                                                               Arrays.stream( SummaryStatistic.StatisticName.values() )
                                                                     .collect( Collectors.toUnmodifiableSet() ) );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                for ( List<DurationScoreStatisticOuter> nextStatistics : grouped )
                {
                    JFreeChart chart = chartFactory.getDurationScoreChart( nextStatistics,
                                                                           helper.getDurationUnits() );

                    DurationScoreStatisticOuter next = nextStatistics.get( 0 );
                    MetricConstants metricName = next.getMetricName();
                    PoolMetadata metadata = next.getPoolMetadata();
                    String pathQualifier = DurationScoreGraphicsWriter.getPathQualifier( nextStatistics );

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
        catch ( ChartBuildingException e )
        {
            throw new GraphicsWriteException( "Error while generating duration score charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Generates a path qualifier for the graphic based on the statistics provided.
     * @param statistics the statistics
     * @return a path qualifier or null if non is required
     */

    private static String getPathQualifier( List<DurationScoreStatisticOuter> statistics )
    {
        String append = "";

        // Qualify by summary statistic
        Optional<SummaryStatistic> statistic =
                statistics.stream()
                          .filter( n -> n.isSummaryStatistic()
                                        && n.getSummaryStatistic()
                                            .getDimension()
                                           != SummaryStatistic.StatisticDimension.RESAMPLED )
                          .map( DurationScoreStatisticOuter::getSummaryStatistic )
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

    private DurationScoreGraphicsWriter( Outputs outputsDescription,
                                         Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
