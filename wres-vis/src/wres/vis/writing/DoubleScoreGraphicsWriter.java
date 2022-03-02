package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import org.jfree.chart.JFreeChart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DataFactory;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.statistics.generated.Outputs;
import wres.vis.charts.ChartBuildingException;
import wres.vis.charts.ChartFactory;

/**
 * Helps write charts comprising {@link DoubleScoreStatisticOuter} to graphics formats.
 * 
 * @author James Brown
 */

public class DoubleScoreGraphicsWriter extends GraphicsWriter
        implements Function<List<DoubleScoreStatisticOuter>, Set<Path>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DoubleScoreGraphicsWriter.class );

    /**
     * Returns an instance of a writer.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static DoubleScoreGraphicsWriter of( Outputs outputsDescription,
                                                Path outputDirectory )
    {
        return new DoubleScoreGraphicsWriter( outputsDescription, outputDirectory );
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
    public Set<Path> apply( List<DoubleScoreStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing duration score outputs." );

        Set<Path> paths = new HashSet<>();

        // Iterate through each metric 
        SortedSet<MetricConstants> metrics = Slicer.discover( output, DoubleScoreStatisticOuter::getMetricName );
        for ( MetricConstants next : metrics )
        {
            if ( next == MetricConstants.CONTINGENCY_TABLE )
            {
                LOGGER.debug( "Discovered contingency table output while writing PNGs: ignoring these outputs." );
            }
            else
            {
                List<DoubleScoreStatisticOuter> filtered = Slicer.filter( output, next );

                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<LeftOrRightOrBaseline, List<DoubleScoreStatisticOuter>> groups =
                        Slicer.getStatisticsGroupedByContext( filtered );

                for ( List<DoubleScoreStatisticOuter> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            DoubleScoreGraphicsWriter.writeScoreCharts( super.getOutputDirectory(),
                                                                        super.getOutputsDescription(),
                                                                        nextGroup );
                    paths.addAll( innerPathsWrittenTo );
                }
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Writes a set of charts associated with {@link DoubleScoreStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the outputs required
     * @param output the metric output
     * @return the paths written
     * @throws GraphicsWriteException when an error occurs during writing
     */

    private static Set<Path> writeScoreCharts( Path outputDirectory,
                                               Outputs outputsDescription,
                                               List<DoubleScoreStatisticOuter> output )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        ChartFactory chartFactory = GraphicsWriter.getChartFactory();

        // Build charts
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            PoolMetadata metadata = output.get( 0 ).getMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutputs : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutputs );

                // As many outputs as secondary thresholds if secondary thresholds are defined
                // and the output type is OutputTypeSelection.THRESHOLD_LEAD.
                List<List<DoubleScoreStatisticOuter>> allOutputs = new ArrayList<>();

                SortedSet<ThresholdOuter> secondThreshold =
                        Slicer.discover( output, next -> next.getMetadata().getThresholds().second() );

                if ( !secondThreshold.isEmpty() )
                {
                    // Slice by the second threshold
                    secondThreshold.forEach( next -> allOutputs.add( Slicer.filter( output,
                                                                                    value -> next.equals( value.getMetadata()
                                                                                                               .getThresholds()
                                                                                                               .second() ) ) ) );
                }
                // One output only
                else
                {
                    allOutputs.add( output );
                }

                for ( List<DoubleScoreStatisticOuter> nextOutput : allOutputs )
                {
                    Map<MetricConstants, JFreeChart> engines = chartFactory.getScoreCharts( nextOutput,
                                                                                            helper.getGraphicShape(),
                                                                                            helper.getDurationUnits() );
                    String append = null;

                    // Secondary threshold? If yes, only one, as this was sliced above
                    SortedSet<ThresholdOuter> second =
                            Slicer.discover( nextOutput,
                                             next -> next.getMetadata().getThresholds().second() );
                    if ( !second.isEmpty() )
                    {
                        append = second.iterator().next().toStringSafe();
                    }

                    Set<Path> paths = DoubleScoreGraphicsWriter.writeNextGroupOfDestinations( outputDirectory,
                                                                                              metadata,
                                                                                              engines,
                                                                                              metricName,
                                                                                              append,
                                                                                              nextOutputs );
                    pathsWrittenTo.addAll( paths );
                }
            }
        }
        catch ( ChartBuildingException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating double score charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     *  Writes a group of destinations.
     * 
     * @param outputDirectory the output directory
     * @param metadata the sample metadata
     * @param engines the graphics engines
     * @param metricName the metric name
     * @param append a string to append to the path
     * @param outputsDescription a description of the outputs required
     * @return the paths written
     * @throws IOException if the graphic could not be created or written
     * @throws ChartBuildingException if the chart could not be created
     */

    private static Set<Path> writeNextGroupOfDestinations( Path outputDirectory,
                                                           PoolMetadata metadata,
                                                           Map<MetricConstants, JFreeChart> engines,
                                                           MetricConstants metricName,
                                                           String append,
                                                           Outputs outputsDescription )
            throws IOException
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build the outputs
        for ( final Entry<MetricConstants, JFreeChart> nextEntry : engines.entrySet() )
        {

            // Qualify with the component name unless there is one component and it is main
            MetricConstants componentName = null;
            if ( nextEntry.getKey() != MetricConstants.MAIN || engines.size() > 0 )
            {
                componentName = nextEntry.getKey();
            }

            // Build the output file name
            Path outputImage = DataFactory.getPathFromPoolMetadata( outputDirectory,
                                                                    metadata,
                                                                    append,
                                                                    metricName,
                                                                    componentName );

            JFreeChart chart = nextEntry.getValue();

            // Write formats
            Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                   chart,
                                                                   outputsDescription );

            pathsWrittenTo.addAll( finishedPaths );
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

    private DoubleScoreGraphicsWriter( Outputs outputsDescription,
                                       Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
