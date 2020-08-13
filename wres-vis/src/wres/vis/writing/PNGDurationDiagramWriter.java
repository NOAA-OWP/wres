package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.vis.ChartEngineFactory;
import wres.vis.config.ConfigHelper;

/**
 * Helps write charts comprising {@link DurationDiagramStatisticOuter} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGDurationDiagramWriter extends PNGWriter
        implements Consumer<List<DurationDiagramStatisticOuter>>,
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

    public static PNGDurationDiagramWriter of( ProjectConfigPlus projectConfigPlus,
                                               ChronoUnit durationUnits,
                                               Path outputDirectory )
    {
        return new PNGDurationDiagramWriter( projectConfigPlus, durationUnits, outputDirectory );
    }

    /**
     * Writes all output for one paired type.
     *
     * @param output the paired output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final List<DurationDiagramStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ProjectConfigs.getGraphicalDestinations( super.getProjectConfigPlus().getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {

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
                            PNGDurationDiagramWriter.writePairedOutputByInstantDurationCharts( super.getOutputDirectory(),
                                                                                               super.getProjectConfigPlus(),
                                                                                               destinationConfig,
                                                                                               nextGroup,
                                                                                               super.getDurationUnits() );
                    this.pathsWrittenTo.addAll( innerPathsWrittenTo );
                }
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
     * Writes a set of charts associated with {@link DurationDiagramStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results
     * @param durationUnits the time units for durations
     * @throws PNGWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writePairedOutputByInstantDurationCharts( Path outputDirectory,
                                                                       ProjectConfigPlus projectConfigPlus,
                                                                       DestinationConfig destinationConfig,
                                                                       List<DurationDiagramStatisticOuter> output,
                                                                       ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            SampleMetadata metadata = output.get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, metricName );

            final ChartEngine engine =
                    ChartEngineFactory.buildDurationDiagramChartEngine( projectConfigPlus.getProjectConfig(),
                                                                        output,
                                                                        helper.getTemplateResourceName(),
                                                                        helper.getGraphicsString(),
                                                                        durationUnits );

            // Build the output file name
            Path outputImage = ConfigHelper.getOutputPathToWrite( outputDirectory,
                                                                  destinationConfig,
                                                                  metadata,
                                                                  metricName,
                                                                  null );

            PNGWriter.writeChart( outputImage, engine, destinationConfig );
            // Only if writeChart succeeded do we assume that it was written
            pathsWrittenTo.add( outputImage );
        }
        catch ( ChartEngineException | IOException e )
        {
            throw new PNGWriteException( "Error while generating multi-vector charts: ", e );
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

    private PNGDurationDiagramWriter( ProjectConfigPlus projectConfigPlus,
                                      ChronoUnit durationUnits,
                                      Path outputDirectory )
    {
        super( projectConfigPlus, durationUnits, outputDirectory );
    }

}
