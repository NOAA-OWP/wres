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
import wres.datamodel.DataUtilities;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.statistics.generated.Outputs;
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
     * @throws ProjectConfigException if the project configuration is not valid for writing
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
            // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
            // each window with LeftOrRightOrBaseline.BASELINE data): #48287
            Map<LeftOrRightOrBaseline, List<DurationDiagramStatisticOuter>> groups =
                    Slicer.getStatisticsGroupedByContext( filtered );

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
            MetricConstants metricName = statistics.get( 0 ).getMetricName();
            PoolMetadata metadata = statistics.get( 0 ).getMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );

                JFreeChart chart = chartFactory.getDurationDiagramChart( statistics,
                                                                    helper.getDurationUnits() );

                // Build the output file name
                Path outputImage = DataUtilities.getPathFromPoolMetadata( outputDirectory,
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
            throw new GraphicsWriteException( "Error while generating duration diagram charts: ", e );
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

    private DurationDiagramGraphicsWriter( Outputs outputsDescription,
                                           Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
