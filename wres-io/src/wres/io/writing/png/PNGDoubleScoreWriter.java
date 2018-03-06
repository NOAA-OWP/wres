package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DoubleScoreOutput} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class PNGDoubleScoreWriter extends PNGWriter
        implements Consumer<MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGDoubleScoreWriter of( final ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
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
    public void accept( final MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( projectConfigPlus.getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {

            // Iterate through each metric 
            for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> e : output.entrySet() )
            {
                PNGDoubleScoreWriter.writeScoreCharts( projectConfigPlus, destinationConfig, e.getValue() );
            }

        }
    }

    /**
     * Writes a set of charts associated with {@link DoubleScoreOutput} for a single metric and time window,
     * stored in a {@link MetricOutputMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric output
     * @throws PNGWriteException when an error occurs during writing
     */

    private static void writeScoreCharts( ProjectConfigPlus projectConfigPlus,
                                          DestinationConfig destinationConfig,
                                          MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> output )
    {
        // Build charts
        try
        {
            MetricOutputMetadata meta = output.getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            ConcurrentMap<MetricConstants, ChartEngine> engines =
                    ChartEngineFactory.buildScoreOutputChartEngine( projectConfigPlus.getProjectConfig(),
                                                                    output,
                                                                    DATA_FACTORY,
                                                                    helper.getOutputType(),
                                                                    helper.getTemplateResourceName(),
                                                                    helper.getGraphicsString() );

            // Build the outputs
            for ( final Entry<MetricConstants, ChartEngine> nextEntry : engines.entrySet() )
            {

                // Build the output file name
                Path outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig, meta );

                PNGWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
            }

        }
        catch ( ChartEngineException | IOException e )
        {
            throw new PNGWriteException( "Error while generating multi-vector charts: ", e );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfigPlus the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private PNGDoubleScoreWriter( ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
    {
        super( projectConfigPlus );
    }

}
