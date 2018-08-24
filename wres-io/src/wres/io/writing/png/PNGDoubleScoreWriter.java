package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.OutputTypeSelection;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.thresholds.Threshold;
import wres.io.config.ConfigHelper;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DoubleScoreStatistic} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGDoubleScoreWriter extends PNGWriter
        implements Consumer<ListOfStatistics<DoubleScoreStatistic>>,
                   Supplier<Set<Path>>
{
    private Set<Path> pathsWrittenTo = new HashSet<>();

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGDoubleScoreWriter of( final ProjectConfigPlus projectConfigPlus )
    {
        return new PNGDoubleScoreWriter( projectConfigPlus );
    }

    /**
     * Writes all output for one score type.
     *
     * @param output the score output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final ListOfStatistics<DoubleScoreStatistic> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( projectConfigPlus.getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {

            // Iterate through each metric 
            SortedSet<MetricConstants> metrics = Slicer.discover( output, meta -> meta.getMetadata().getMetricID() );
            for ( MetricConstants next : metrics )
            {
                Set<Path> innerPathsWrittenTo =
                        PNGDoubleScoreWriter.writeScoreCharts( projectConfigPlus,
                                                               destinationConfig,
                                                               Slicer.filter( output, next ) );
                this.pathsWrittenTo.addAll( innerPathsWrittenTo );
            }
        }
    }


    /**
     *
     * @return paths written to *so far*
     */

    @Override
    public Set<Path> get()
    {
        return Collections.unmodifiableSet( this.pathsWrittenTo );
    }

    /**
     * Writes a set of charts associated with {@link DoubleScoreStatistic} for a single metric and time window,
     * stored in a {@link ListOfStatistics}.
     *
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric output
     * @throws PNGWriteException when an error occurs during writing
     * @return the paths actually written to
     */

    private static Set<Path> writeScoreCharts( ProjectConfigPlus projectConfigPlus,
                                               DestinationConfig destinationConfig,
                                               ListOfStatistics<DoubleScoreStatistic> output )
    {
        Set<Path> pathsWrittenTo = new HashSet<>();

        // Build charts
        try
        {
            StatisticMetadata meta = output.getData().get( 0 ).getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            // As many outputs as secondary thresholds if secondary thresholds are defined
            // and the output type is OutputTypeSelection.THRESHOLD_LEAD.
            List<ListOfStatistics<DoubleScoreStatistic>> allOutputs = new ArrayList<>();

            SortedSet<Threshold> secondThreshold =
                    Slicer.discover( output, next -> next.getMetadata().getThresholds().second() );

            if ( destinationConfig.getOutputType() == OutputTypeSelection.THRESHOLD_LEAD
                 && !secondThreshold.isEmpty() )
            {
                // Slice by the second threshold
                secondThreshold.forEach( next -> allOutputs.add( Slicer.filter( output,
                                                                                value -> next.equals( value.getThresholds()
                                                                                                           .second() ) ) ) );
            }
            // One output only
            else
            {
                allOutputs.add( output );
            }

            for ( ListOfStatistics<DoubleScoreStatistic> nextOutput : allOutputs )
            {
                ConcurrentMap<MetricConstants, ChartEngine> engines =
                        ChartEngineFactory.buildScoreOutputChartEngine( projectConfigPlus.getProjectConfig(),
                                                                        nextOutput,
                                                                        helper.getOutputType(),
                                                                        helper.getTemplateResourceName(),
                                                                        helper.getGraphicsString() );

                String append = null;

                // Secondary threshold? If yes, only, one as this was sliced above
                SortedSet<Threshold> second =
                        Slicer.discover( nextOutput, next -> next.getMetadata().getThresholds().second() );
                if ( !second.isEmpty() )
                {
                    append = second.iterator().next().toStringSafe();
                }

                // Build the outputs
                for ( final Entry<MetricConstants, ChartEngine> nextEntry : engines.entrySet() )
                {

                    // Build the output file name
                    Path outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig, meta, append );

                    PNGWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
                    // Only if writeChart succeeded do we assume that it was written
                    pathsWrittenTo.add( outputImage );
                }
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
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private PNGDoubleScoreWriter( ProjectConfigPlus projectConfigPlus )
    {
        super( projectConfigPlus );
    }

}
