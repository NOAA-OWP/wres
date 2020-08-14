package wres.vis.writing;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.graphics2d.svg.SVGGraphics2D;
import org.jfree.graphics2d.svg.SVGUtils;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.config.ProjectConfigPlus;
import wres.config.ProjectConfigs;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;

/**
 * Helps to write a {@link ChartEngine} to a graphic in various formats.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class GraphicsWriter
{

    /**
     * Default chart height in pixels.
     */

    private static final int DEFAULT_CHART_HEIGHT = 600;

    /**
     * Default chart width in pixels.
     */

    private static final int DEFAULT_CHART_WIDTH = 800;

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
     * @return the path actually written
     * @throws GraphicsWriteException if the chart could not be written
     */

    static Path writeChart( Path outputImage,
                            ChartEngine engine,
                            DestinationConfig dest )
    {
        int width = GraphicsWriter.DEFAULT_CHART_WIDTH;
        int height = GraphicsWriter.DEFAULT_CHART_HEIGHT;

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
            Path resolvedPath = outputImage;

            // Default is png
            if ( dest.getType() == DestinationType.GRAPHIC || dest.getType() == DestinationType.PNG )
            {
                resolvedPath = resolvedPath.resolveSibling( resolvedPath.getFileName() + ".png" );
                File outputImageFile = resolvedPath.toFile();

                // #58735-18
                ChartUtilities.saveChartAsPNG( outputImageFile, engine.buildChart(), width, height );
            }
            else if ( dest.getType() == DestinationType.SVG )
            {
                resolvedPath = resolvedPath.resolveSibling( resolvedPath.getFileName() + ".svg" );
                File outputImageFile = resolvedPath.toFile();

                // Create the chart
                JFreeChart chart = engine.buildChart();

                // Create the svg string
                SVGGraphics2D svg2d = new SVGGraphics2D( width, height );
                chart.draw( svg2d, new Rectangle2D.Double( 0, 0, width, height ) );
                String svgElement = svg2d.getSVGElement();
                SVGUtils.writeToSVG( outputImageFile, svgElement );
            }
            // No others supported
            else
            {
                throw new UnsupportedOperationException( "The destination type '" + dest.getType()
                                                         + "' is not supported for graphics writing." );
            }

            return resolvedPath;
        }
        catch ( IOException | ChartEngineException | XYChartDataSourceException e )
        {
            throw new GraphicsWriteException( "Error while writing chart:", e );
        }
    }

    /**
     * Helper that groups destinations by their common graphics parameters. Each inner list requires one set of 
     * graphics, which may be written across N formats.
     * 
     * @param destinations the destinations
     * @return the groups of destinations by common graphics parameters
     */

    static Collection<List<DestinationConfig>>
            getDestinationsGroupedByGraphicsParameters( List<DestinationConfig> destinations )
    {
        Objects.requireNonNull( destinations );

        // Map the destination to a string representation of the graphics parameters
        Function<DestinationConfig, String> mapper = destination -> {
            if ( Objects.nonNull( destination.getGraphical() ) )
            {
                return destination.toString();
            }

            return "defaultGraphics";
        };

        // Use the string representation of the GraphicalType
        Map<String, List<DestinationConfig>> destinationsPerGraphic = destinations.stream()
                                                                                  .collect( Collectors.groupingBy( mapper,
                                                                                                                   Collectors.toList() ) );

        return destinationsPerGraphic.values();
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
            String innerGraphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );

            // Build the chart engine
            MetricConfig nextConfig =
                    getNamedConfigOrAllValid( metricId, config );

            // Default to global type parameter
            OutputTypeSelection innerOutputType = OutputTypeSelection.DEFAULT;
            if ( Objects.nonNull( destConfig.getOutputType() ) )
            {
                innerOutputType = destConfig.getOutputType();
            }
            String innerTemplateResourceName = null;

            if ( Objects.nonNull( destConfig.getGraphical() ) )
            {
                innerTemplateResourceName = destConfig.getGraphical().getTemplate();
            }

            // Override template name with metric specific name.
            if ( Objects.nonNull( nextConfig ) && Objects.nonNull( nextConfig.getTemplateResourceName() ) )
            {
                innerTemplateResourceName = nextConfig.getTemplateResourceName();
            }

            this.templateResourceName = innerTemplateResourceName;
            this.outputType = innerOutputType;
            this.graphicsString = innerGraphicsString;
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
            MetricConfig allValid = ProjectConfigs.getMetricConfigByName( config, MetricConfigName.ALL_VALID );
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

    GraphicsWriter( ProjectConfigPlus projectConfigPlus,
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
