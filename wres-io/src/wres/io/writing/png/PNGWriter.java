package wres.io.writing.png;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;

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
import wres.datamodel.MetricConstants;
import wres.io.config.ConfigHelper;
import wres.system.SystemSettings;

/**
 * Helps to write a {@link ChartEngine} to a graphical product file in Portable Network Graphics (PNG) format.
 * 
 * TODO: implementations of this class are currently building a graphical interchange format {@link ChartEngine},
 * which doesn't make sense. For example, implementing a new output format, such as JPEG, would require that the new 
 * writer generated its own copy in the interchange format. Instead, rhe interchange format should be passed to the 
 * format writers. More generally, the graphical interchange format should be something other than {@link ChartEngine}. 
 * The numerical data interchange format is different. See #54731.  
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class PNGWriter
{

    /**
     * Resolution for writing duration outputs.
     */

    private final ChronoUnit durationUnits; 

    /**
     * The project configuration to write.
     */

    private final ProjectConfigPlus projectConfigPlus;

    /**
     * The output directory to use to write
     */
    private final Path outputDirectory;

    /**
     * Returns the duration units for writing lead durations.
     * 
     * @return the duration units
     */
    
    ChronoUnit getDurationUnits()
    {
        return this.durationUnits;
    }
    
    /**
     * Returns the project declaration
     * 
     * @return the project declaration
     */
    
    ProjectConfigPlus getProjectConfigPlus()
    {
        return this.projectConfigPlus;
    }    

    Path getOutputDirectory()
    {
        return this.outputDirectory;
    }

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
        final File outputImageFile = outputImage.toFile();

        // TODO: these defaults should be provided in the declaration
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
            String templateResourceName = null;
            
            if( Objects.nonNull( destConfig.getGraphical() ) )
            {
                templateResourceName = destConfig.getGraphical().getTemplate();
            }

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
            for ( MetricsConfig next : config.getMetrics() )
            {
                Optional<MetricConfig> test =
                        next.getMetric().stream().filter( a -> metric.name().equals( a.getName().name() ) ).findFirst();
                if ( test.isPresent() )
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
     * @param durationUnits the time units for lead durations
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null
     */

    PNGWriter( ProjectConfigPlus projectConfigPlus,
               ChronoUnit durationUnits,
               Path outputDirectory )
    {
        Objects.requireNonNull( projectConfigPlus, "Specify a non-null project declaration." );
        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );

        this.projectConfigPlus = projectConfigPlus;
        this.durationUnits = durationUnits;
        this.outputDirectory = outputDirectory;
    }
}
