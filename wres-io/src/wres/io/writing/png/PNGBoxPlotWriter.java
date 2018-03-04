package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link BoxPlotOutput} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class PNGBoxPlotWriter extends PNGWriter
        implements Consumer<MetricOutputMapByTimeAndThreshold<BoxPlotOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGBoxPlotWriter of( final ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
    {
        return new PNGBoxPlotWriter( projectConfigPlus );
    }

    /**
     * Writes all output for one box plot type.
     *
     * @param output the box plot output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final MetricOutputMapByTimeAndThreshold<BoxPlotOutput> output )
    {
        Objects.requireNonNull( output, "Specify non-null input data when writing diagram outputs." );

        // Write output
        List<DestinationConfig> destinations =
                ConfigHelper.getGraphicalDestinations( projectConfigPlus.getProjectConfig() );
        for ( DestinationConfig destinationConfig : destinations )
        {
            // Build charts
            try
            {
                MetricOutputMetadata meta = output.getMetadata();

                GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destinationConfig, meta.getMetricID() );

                final Map<Pair<TimeWindow, Threshold>, ChartEngine> engines =
                        ChartEngineFactory.buildBoxPlotChartEngine( projectConfigPlus.getProjectConfig(),
                                                                    output,
                                                                    helper.getTemplateResourceName(),
                                                                    helper.getGraphicsString() );

                // Build the outputs
                for ( final Entry<Pair<TimeWindow, Threshold>, ChartEngine> nextEntry : engines.entrySet() )
                {

                    Path outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig,
                                                                          meta,
                                                                          nextEntry.getKey().getLeft() );

                    PNGWriter.writeChart( outputImage, nextEntry.getValue(), destinationConfig );
                }

            }
            catch ( ChartEngineException | IOException e )
            {
                throw new PNGWriteException( "Error while generating multi-vector charts: ", e );
            }
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param projectConfigPlus the project configuration
     * @throws ProjectConfigException if the project configuration is not valid for writing 
     */

    private PNGBoxPlotWriter( ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
    {
        super( projectConfigPlus );
    }

}
