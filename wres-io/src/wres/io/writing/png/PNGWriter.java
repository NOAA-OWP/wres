package wres.io.writing.png;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;

/**
 * Helps to write a {@link ChartEngine} to a graphical product file in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class PNGWriter
{

    /**
     * Logger.
     */

    static final Logger LOGGER = LoggerFactory.getLogger( PNGWriter.class );

    /**
     * Default data factory.
     */

    static final DataFactory DATA_FACTORY = DefaultDataFactory.getInstance();    
    
    /**
     * The project configuration to write.
     */

    final ProjectConfigPlus projectConfigPlus;

    /**
     * Writes an output chart to a specified path.
     * 
     * @param outputImage the path to the output image
     * @param engine the chart engine
     * @param dest the destination configuration
     * @throws PNGWriteException if the chart could not be written
     */

    static void writeChart( final Path outputImage,
                            final ChartEngine engine,
                            final DestinationConfig dest )
            throws PNGWriteException
    {
        if ( LOGGER.isWarnEnabled() && outputImage.toFile().exists() )
        {
            LOGGER.warn( "File {} already exists and is being overwritten.", outputImage );
        }

        final File outputImageFile = outputImage.toFile();

        int width = SystemSettings.getDefaultChartWidth();
        int height = SystemSettings.getDefaultChartHeight();

        if ( dest.getGraphical() != null && dest.getGraphical().getWidth() != null )
        {
            width = dest.getGraphical().getWidth();
        }
        if ( dest.getGraphical() != null && dest.getGraphical().getHeight() != null )
        {
            height = dest.getGraphical().getHeight();
        }
        try
        {
            ChartTools.generateOutputImageFile( outputImageFile, engine.buildChart(), width, height );
        }
        catch ( IOException | ChartEngineException | XYChartDataSourceException e )
        {
            throw new PNGWriteException( "Error while writing chart:", e );
        }
    }

    /**
     * A helper class that builds the parameters required for graphics generation.
     * 
     * @author james.brown@hydrosolved.com
     */

    static class GraphicsHelper
    {

        /**
         * The template resource name.
         */

        private final String templateResourceName;

        /**
         * The graphics string.
         */

        private final String graphicsString;

        /**
         * The output type.
         */

        private final OutputTypeSelection outputType;

        /**
         * Returns a graphics helper.
         *
         * @param projectConfigPlus the project configuration
         * @param destConfig the destination configuration
         * @param metricId the metric identifier
         * @return a graphics helper
         */

        static GraphicsHelper of( ProjectConfigPlus projectConfigPlus,
                                  DestinationConfig destConfig,
                                  MetricConstants metricId )
        {
            return new GraphicsHelper( projectConfigPlus, destConfig, metricId );
        }

        /**
         * Builds a helper.
         *
         * @param projectConfigPlus the project configuration
         * @param destConfig the destination configuration
         * @param metricId the metric identifier
         */

        private GraphicsHelper( ProjectConfigPlus projectConfigPlus,
                                DestinationConfig destConfig,
                                MetricConstants metricId )
        {
            ProjectConfig config = projectConfigPlus.getProjectConfig();
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            
            // Build the chart engine
            MetricConfig nextConfig =
                    getNamedConfigOrAllValid( metricId, config );
            
            // Default to global type parameter
            OutputTypeSelection outputType = OutputTypeSelection.DEFAULT;
            if ( Objects.nonNull( destConfig.getOutputType() ) )
            {
                outputType = destConfig.getOutputType();
            }
            String templateResourceName = destConfig.getGraphical().getTemplate();
            if ( Objects.nonNull( nextConfig ) )
            {

                // Override template name with metric specific name.
                if ( Objects.nonNull( nextConfig.getTemplateResourceName() ) )
                {
                    templateResourceName = nextConfig.getTemplateResourceName();
                }
            }
            this.templateResourceName = templateResourceName;
            this.outputType = outputType;
            this.graphicsString = graphicsString;
        }

        /**
         * Returns the output type.
         * @return the output type
         */

        OutputTypeSelection getOutputType()
        {
            return outputType;
        }

        /**
         * Returns the graphics string.
         * @return the graphics string
         */

        String getGraphicsString()
        {
            return graphicsString;
        }

        /**
         * Returns the template resource name.
         * @return the template resource name
         */

        String getTemplateResourceName()
        {
            return templateResourceName;
        }

        /**
         * Locates the metric configuration corresponding to the input {@link MetricConstants} or null if no corresponding
         * configuration could be found. If the configuration contains a {@link MetricConfigName#ALL_VALID}, the
         * prescribed metric identifier is ignored and the configuration is returned for
         * {@link MetricConfigName#ALL_VALID}.
         *
         * @param metric the metric
         * @param config the project configuration
         * @return the metric configuration or null
         */

        private static MetricConfig getNamedConfigOrAllValid( final MetricConstants metric, final ProjectConfig config )
        {
            
            // Deal with MetricConfigName.ALL_VALID first
            MetricConfig allValid = ConfigHelper.getMetricConfigByName( config, MetricConfigName.ALL_VALID );
            if ( Objects.nonNull( allValid ) )
            {
                return allValid;
            }
            
            // Find the corresponding configuration
            for( MetricsConfig next : config.getMetrics() )
            {
                Optional<MetricConfig> test =
                        next.getMetric().stream().filter( a -> metric.name().equals( a.getName().name() ) ).findFirst();
                if( test.isPresent() )
                {
                    return test.get();
                }
            }
            
            return null;
        } 
    }
    
    /**
     * Hidden constructor.
     * 
     * @param projectConfigPlus the project configuration
     */

    PNGWriter( ProjectConfigPlus projectConfigPlus )
    {
        this.projectConfigPlus = projectConfigPlus;
    }
}
