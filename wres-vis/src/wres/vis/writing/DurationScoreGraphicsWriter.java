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

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.pools.SampleMetadata;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DurationScoreStatisticOuter} to graphics formats.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DurationScoreGraphicsWriter extends GraphicsWriter
        implements Function<List<DurationScoreStatisticOuter>,Set<Path>>
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
            // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
            // each window with LeftOrRightOrBaseline.BASELINE data): #48287
            Map<LeftOrRightOrBaseline, List<DurationScoreStatisticOuter>> groups =
                    Slicer.getStatisticsGroupedByContext( filtered );

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
     * @param output the metric output
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeScoreCharts( Path outputDirectory,
                                               Outputs outputsDescription,
                                               List<DurationScoreStatisticOuter> output )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            SampleMetadata metadata = output.get( 0 ).getMetadata();

            // Collection of graphics parameters, one for each set of charts to write across N formats.
            Collection<Outputs> outputsMap =
                    GraphicsWriter.getOutputsGroupedByGraphicsParameters( outputsDescription );

            for ( Outputs nextOutput : outputsMap )
            {
                // One helper per set of graphics parameters.
                GraphicsHelper helper = GraphicsHelper.of( nextOutput );
                
                ChartEngine engine =
                        ChartEngineFactory.buildCategoricalDurationScoreChartEngine( output,
                                                                                     helper.getGraphicShape(),
                                                                                     helper.getTemplateResourceName(),
                                                                                     helper.getGraphicsString(),
                                                                                     helper.getDurationUnits() );


                // Build the output file name
                Path outputImage = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                          metadata,
                                                                          metricName,
                                                                          null );

                // Write formats
                Set<Path> finishedPaths = GraphicsWriter.writeGraphic( outputImage,
                                                                       engine,
                                                                       nextOutput );

                pathsWrittenTo.addAll( finishedPaths );
            }
        }
        catch ( ChartEngineException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating duration score charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     *
     * @param outputsDescription a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @param durationUnits the time units for durations
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private DurationScoreGraphicsWriter( Outputs outputsDescription,
                                         Path outputDirectory )
    {
        super( outputsDescription, outputDirectory );
    }

}
