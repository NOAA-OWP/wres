package wres.io.writing.png;

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
import wres.config.generated.DestinationConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.io.config.ConfigHelper;
import wres.io.writing.WriterHelper;
import wres.system.SystemSettings;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DurationScoreStatisticOuter} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGDurationScoreWriter extends PNGWriter
        implements Consumer<List<DurationScoreStatisticOuter>>,
        Supplier<Set<Path>>
{
    private Set<Path> pathsWrittenTo = new HashSet<>();

    /**
     * Returns an instance of a writer.
     *
     * @param systemSettings The system settings to use.
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @param outputDirectory the directory into which to write
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGDurationScoreWriter of( SystemSettings systemSettings,
                                             ProjectConfigPlus projectConfigPlus,
                                             ChronoUnit durationUnits,
                                             Path outputDirectory )
    {
        return new PNGDurationScoreWriter( systemSettings,
                                           projectConfigPlus,
                                           durationUnits,
                                           outputDirectory );
    }

    /**
     * Writes all output for one score type.
     *
     * @param output the score output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final List<DurationScoreStatisticOuter> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( super.getProjectConfigPlus().getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {
            // Iterate through each metric 
            SortedSet<MetricConstants> metrics = Slicer.discover( output, DurationScoreStatisticOuter::getMetricName );
            for ( MetricConstants next : metrics )
            {
                List<DurationScoreStatisticOuter> filtered = Slicer.filter( output, next );

                // Group the statistics by the LRB context in which they appear. There will be one path written
                // for each group (e.g., one path for each window with LeftOrRightOrBaseline.RIGHT data and one for 
                // each window with LeftOrRightOrBaseline.BASELINE data): #48287
                Map<LeftOrRightOrBaseline, List<DurationScoreStatisticOuter>> groups =
                        WriterHelper.getStatisticsGroupedByContext( filtered );

                for ( List<DurationScoreStatisticOuter> nextGroup : groups.values() )
                {
                    Set<Path> innerPathsWrittenTo =
                            PNGDurationScoreWriter.writeScoreCharts( super.getSystemSettings(),
                                                                     super.getOutputDirectory(),
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
     * Writes a set of charts associated with {@link DurationScoreStatisticOuter} for a single metric and time window,
     * stored in a {@link List}.
     *
     * @param systemSettings The system settings to use.
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric output
     * @param durationUnits the time units for durations
     * @throws PNGWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeScoreCharts( SystemSettings systemSettings,
                                               Path outputDirectory,
                                               ProjectConfigPlus projectConfigPlus,
                                               DestinationConfig destinationConfig,
                                               List<DurationScoreStatisticOuter> output,
                                               ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            MetricConstants metricName = output.get( 0 ).getMetricName();
            SampleMetadata metadata = output.get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, metricName );

            ChartEngine engine =
                    ChartEngineFactory.buildCategoricalDurationScoreChartEngine( projectConfigPlus.getProjectConfig(),
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

            PNGWriter.writeChart( systemSettings, outputImage, engine, destinationConfig );
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
     * @param outputDirectory the directory into which to write
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private PNGDurationScoreWriter( SystemSettings systemSettings,
                                    ProjectConfigPlus projectConfigPlus,
                                    ChronoUnit durationUnits,
                                    Path outputDirectory )
    {
        super( systemSettings, projectConfigPlus, durationUnits, outputDirectory );
    }

}
