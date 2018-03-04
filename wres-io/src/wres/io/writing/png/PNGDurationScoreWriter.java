package wres.io.writing.png;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.vis.ChartEngineFactory;

/**
 * Helps write charts comprising {@link DurationScoreOutput} to a file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 1.0
 */

public class PNGDurationScoreWriter extends PNGWriter
        implements Consumer<MetricOutputMapByTimeAndThreshold<DurationScoreOutput>>
{

    /**
     * Returns an instance of a writer.
     * 
     * @param projectConfigPlus the project configuration
     * @return a writer
     * @throws NullPointerException if the input is null 
     * @throws ProjectConfigException if the project configuration is not valid for writing
     */

    public static PNGDurationScoreWriter of( final ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
    {
        return new PNGDurationScoreWriter( projectConfigPlus );
    }

    /**
     * Writes all output for one score type.
     *
     * @param output the score output
     * @throws NullPointerException if the input is null
     * @throws PNGWriteException if the output cannot be written
     */

    @Override
    public void accept( final MetricOutputMapByTimeAndThreshold<DurationScoreOutput> output )
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

                ChartEngine engine =
                        ChartEngineFactory.buildCategoricalDurationScoreChartEngine( projectConfigPlus.getProjectConfig(),
                                                                                     output,
                                                                                     helper.getTemplateResourceName(),
                                                                                     helper.getGraphicsString() );


                // Build the output file name
                Path outputImage = ConfigHelper.getOutputPathToWrite( destinationConfig, meta );

                PNGWriter.writeChart( outputImage, engine, destinationConfig );

            }
            catch ( XYChartDataSourceException | ChartEngineException | IOException e )
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

    private PNGDurationScoreWriter( ProjectConfigPlus projectConfigPlus ) throws ProjectConfigException
    {
        super( projectConfigPlus );
    }

}
