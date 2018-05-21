package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.PairedOutput;
import wres.io.config.ConfigHelper;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link PairedOutput} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class PNGPairedWriter extends PNGWriter
        implements Consumer<MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGPairedWriter of( final ProjectConfigPlus projectConfigPlus )
    {
        return new PNGPairedWriter( projectConfigPlus );
    }

    /**
     * Writes all output for one paired type.
     *
     * @param output the paired output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( projectConfigPlus.getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {

            // Iterate through each metric 
            for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>>> e : output.entrySet() )
            {
                PNGPairedWriter.writePairedOutputByInstantDurationCharts( projectConfigPlus,
                                                                          destinationConfig,
                                                                          e.getValue() );
            }

        }
    }

    /**
     * Writes a set of charts associated with {@link PairedOutput} for a single metric and time window,
     * stored in a {@link MetricOutputMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results
     * @throws PNGWriteException when an error occurs during writing
     */

    private static void writePairedOutputByInstantDurationCharts( ProjectConfigPlus projectConfigPlus,
                                                                  DestinationConfig destinationConfig,
                                                                  MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> output )
    {
        // Build charts
        try
        {
            MetricOutputMetadata meta = output.getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            final ChartEngine engine =
                    ChartEngineFactory.buildPairedInstantDurationChartEngine( projectConfigPlus.getProjectConfig(),
                                                                              output,
                                                                              helper.getTemplateResourceName(),
                                                                              helper.getGraphicsString() );

            // Build the output file name
            Path outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig, meta );

            PNGWriter.writeChart( outputImage, engine, destinationConfig );
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

    private PNGPairedWriter( ProjectConfigPlus projectConfigPlus )
    {
        super( projectConfigPlus );
    }

}
