package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
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
import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MultiVectorStatistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.io.config.ConfigHelper;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link MultiVectorStatistic} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGDiagramWriter extends PNGWriter
        implements Consumer<ListOfStatistics<MultiVectorStatistic>>,
                   Supplier<Set<Path>>
{
    private Set<Path> pathsWrittenTo = new HashSet<>();

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @param durationUnits the time units for durations
     * @return a writer
     * @throws NullPointerException if either input is null
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGDiagramWriter of( final ProjectConfigPlus projectConfigPlus, final ChronoUnit durationUnits )
    {
        return new PNGDiagramWriter( projectConfigPlus, durationUnits );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param output the diagram output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfStatistics<MultiVectorStatistic> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( this.getProjectConfigPlus().getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {
            // Iterate through each metric 
            SortedSet<MetricConstants> metrics = Slicer.discover( output, meta -> meta.getMetadata().getMetricID() );
            for ( MetricConstants next : metrics )
            {
                Set<Path> innerPathsWrittenTo =
                        PNGDiagramWriter.writeMultiVectorCharts( this.getProjectConfigPlus(),
                                                                 destinationConfig,
                                                                 Slicer.filter( output, next ),
                                                                 this.getDurationUnits() );
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
     * Writes a set of charts associated with {@link MultiVectorStatistic} for a single metric and time window,
     * stored in a {@link ListOfStatistics}.
     *
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results
     * @param durationUnits the time units for durations
     * @throws PNGWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeMultiVectorCharts( ProjectConfigPlus projectConfigPlus,
                                                     DestinationConfig destinationConfig,
                                                     ListOfStatistics<MultiVectorStatistic> output,
                                                     ChronoUnit durationUnits )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            StatisticMetadata meta = output.getData().get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            final Map<Object, ChartEngine> engines =
                    ChartEngineFactory.buildMultiVectorOutputChartEngine( projectConfigPlus.getProjectConfig(),
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
                if ( append instanceof TimeWindow )
                {
                    outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                                     meta,
                                                                     (TimeWindow) append,
                                                                     durationUnits );
                }
                else if ( append instanceof OneOrTwoThresholds )
                {
                    outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                                     meta,
                                                                     (OneOrTwoThresholds) append );
                }
                else
                {
                    throw new UnsupportedOperationException( "Unexpected situation where WRES could not create outputImage path" );
                }

                PNGWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
                // Only if writeChart succeeded do we assume that it was written
                pathsWrittenTo.add( outputImage );
            }
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
     * @throws ProjectConfigException if the project configuration is not valid for writing
     * @throws NullPointerException if either input is null
     */

    private PNGDiagramWriter( ProjectConfigPlus projectConfigPlus, ChronoUnit durationUnits )
    {
        super( projectConfigPlus, durationUnits );
    }

}
