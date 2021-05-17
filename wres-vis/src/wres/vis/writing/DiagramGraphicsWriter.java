package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Path;
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

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DataFactory;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Outputs;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DiagramStatisticOuter} to graphics formats.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DiagramGraphicsWriter extends GraphicsWriter
        implements Function<List<DiagramStatisticOuter>,Set<Path>>
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

            // Group the statistics by the LRB context in which they appear. There will be one path written
            // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
            // each window with LeftOrRightOrBaseline.BASELINE data): #48287
            Map<LeftOrRightOrBaseline, List<DiagramStatisticOuter>> groups =
                    Slicer.getStatisticsGroupedByContext( filtered );

            for ( List<DiagramStatisticOuter> nextGroup : groups.values() )
            {
                Set<Path> innerPathsWrittenTo =
                        DiagramGraphicsWriter.writeDiagrams( super.getOutputDirectory(),
                                                             super.getOutputsDescription(),
                                                             nextGroup );
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
     * @param output the metric results
     * @return the paths written
     * @throws GraphicsWriteException when an error occurs during writing
     */

    private static Set<Path> writeDiagrams( Path outputDirectory,
                                            Outputs outputsDescription,
                                            List<DiagramStatisticOuter> output )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
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
                
                Map<Object, ChartEngine> engines =
                        ChartEngineFactory.buildDiagramChartEngine( output,
                                                                    helper.getGraphicShape(),
                                                                    helper.getTemplateResourceName(),
                                                                    helper.getGraphicsString(),
                                                                    helper.getDurationUnits() );

                // Build the outputs
                for ( Entry<Object, ChartEngine> nextEntry : engines.entrySet() )
                {
                    // Build the output file name
                    Path outputImage = null;
                    Object append = nextEntry.getKey();
                    if ( append instanceof TimeWindowOuter )
                    {
                        outputImage = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                             metadata,
                                                                             (TimeWindowOuter) append,
                                                                             helper.getDurationUnits(),
                                                                             metricName,
                                                                             null );
                    }
                    else if ( append instanceof OneOrTwoThresholds )
                    {
                        outputImage = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                             metadata,
                                                                             (OneOrTwoThresholds) append,
                                                                             metricName,
                                                                             null );
                    }
                    else
                    {
                        throw new UnsupportedOperationException( "Unexpected situation where WRES could not create "
                                + "outputImage path" );
                    }

                    // Write formats
                    Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                           nextEntry.getValue(),
                                                                           nextOutput );

                    pathsWrittenTo.addAll( finishedPaths );
                }
            }
        }
        catch ( ChartEngineException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating diagram charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     *
     * @param outputDirectory the directory into which to write
     * @param outputsDescription a description of the required outputs
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private DiagramGraphicsWriter( Outputs outputsDescription,
                                   Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
