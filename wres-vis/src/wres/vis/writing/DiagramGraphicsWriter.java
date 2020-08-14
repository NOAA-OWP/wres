package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
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

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.DestinationConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DiagramStatisticOuter} to graphics formats.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DiagramGraphicsWriter extends GraphicsWriter
        implements Consumer<List<DiagramStatisticOuter>>,
        Supplier<Set<Path>>
{
    private Set<Path> pathsWrittenTo = new HashSet<>();

    /**
     * Returns an instance of a writer.
     *
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static DiagramGraphicsWriter of( ProjectConfigPlus projectConfigPlus,
                                            ChronoUnit durationUnits,
                                            Path outputDirectory )
    {
        return new DiagramGraphicsWriter( projectConfigPlus, durationUnits, outputDirectory );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param output the diagram output
     * @throws NullPointerException if the input is null
     * @throws GraphicsWriteException if the output cannot be written
     */

    @Override
    public void accept( List<DiagramStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ProjectConfigs.getGraphicalDestinations( super.getProjectConfigPlus().getProjectConfig() );

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
                                                             super.getProjectConfigPlus(),
                                                             destinations,
                                                             nextGroup,
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
     * Writes a set of charts associated with {@link DiagramStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinations the destinations for the written output
     * @param output the metric results
     * @param durationUnits the time units for durations
     * @throws GraphicsWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeDiagrams( Path outputDirectory,
                                            ProjectConfigPlus projectConfigPlus,
                                            List<DestinationConfig> destinations,
                                            List<DiagramStatisticOuter> output,
                                            ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            SampleMetadata metadata = output.get( 0 ).getMetadata();

            // Map by graphics parameters. Each pair requires a separate chart, written N times across N formats.
            Collection<List<DestinationConfig>> destinationMap =
                    GraphicsWriter.getDestinationsGroupedByGraphicsParameters( destinations );

            for ( List<DestinationConfig> nextDestinations : destinationMap )
            {
                // Each of the inner lists has common graphics parameters, so a common helper
                GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, nextDestinations.get( 0 ), metricName );

                Map<Object, ChartEngine> engines =
                        ChartEngineFactory.buildDiagramChartEngine( projectConfigPlus.getProjectConfig(),
                                                                    output,
                                                                    helper.getOutputType(),
                                                                    helper.getTemplateResourceName(),
                                                                    helper.getGraphicsString(),
                                                                    durationUnits );

                // Build the outputs
                for ( final Entry<Object, ChartEngine> nextEntry : engines.entrySet() )
                {
                    // Build the output file name
                    Path outputImage = null;
                    Object append = nextEntry.getKey();
                    if ( append instanceof TimeWindowOuter )
                    {
                        outputImage = DataFactory.getPathFromSampleMetadata( outputDirectory,
                                                                             metadata,
                                                                             (TimeWindowOuter) append,
                                                                             durationUnits,
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
                        throw new UnsupportedOperationException( "Unexpected situation where WRES could not create outputImage path" );
                    }

                    // Iterate through destinations
                    for ( DestinationConfig destinationConfig : nextDestinations )
                    {
                        Path finishedPath =
                                GraphicsWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
                        // Only if writeChart succeeded do we assume that it was written
                        pathsWrittenTo.add( finishedPath );
                    }
                }
            }
        }
        catch ( ChartEngineException | IOException e )
        {
            throw new GraphicsWriteException( "Error while generating multi-vector charts: ", e );
        }

        return Collections.unmodifiableSet( pathsWrittenTo );
    }

    /**
     * Hidden constructor.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private DiagramGraphicsWriter( ProjectConfigPlus projectConfigPlus,
                                   ChronoUnit durationUnits,
                                   Path outputDirectory )
    {
        super( projectConfigPlus, durationUnits, outputDirectory );
    }

}
