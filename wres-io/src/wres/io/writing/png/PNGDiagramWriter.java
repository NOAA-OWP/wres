package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.Thresholds;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link MultiVectorOutput} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class PNGDiagramWriter extends PNGWriter
        implements Consumer<MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGDiagramWriter of( final ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
    {
        return new PNGDiagramWriter( projectConfigPlus );
    }

    /**
     * Writes all output for one diagram type.
     *
     * @param output the diagram output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( projectConfigPlus.getProjectConfig() );

        // Iterate through destinations
        for ( DestinationConfig destinationConfig : destinations )
        {

            // Iterate through each metric 
            for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>> e : output.entrySet() )
            {
                PNGDiagramWriter.writeMultiVectorCharts( projectConfigPlus, destinationConfig, e.getValue() );
            }

        }

    }

    /**
     * Writes a set of charts associated with {@link MultiVectorOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param destinationConfig the destination configuration for the written output
     * @param output the metric results
     * @throws PNGWriteException when an error occurs during writing
     */

    private static void writeMultiVectorCharts( ProjectConfigPlus projectConfigPlus,
                                                DestinationConfig destinationConfig,
                                                MetricOutputMapByTimeAndThreshold<MultiVectorOutput> output )
    {
        // Build charts
        try
        {
            MetricOutputMetadata meta = output.getMetadata();

            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

            final Map<Object, ChartEngine> engines =
                    ChartEngineFactory.buildMultiVectorOutputChartEngine( projectConfigPlus.getProjectConfig(),
                                                                          output,
                                                                          DATA_FACTORY,
                                                                          helper.getOutputType(),
                                                                          helper.getTemplateResourceName(),
                                                                          helper.getGraphicsString() );

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
                                                                     (TimeWindow) append );
                }
                else if ( append instanceof Thresholds )
                {
                    outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                                     meta,
                                                                     (Thresholds) append );
                }

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

    private PNGDiagramWriter( ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
    {
        super( projectConfigPlus );
    }

}
