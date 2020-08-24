package wres.vis.writing;

import java.awt.Font;
import java.awt.FontFormatException;
import java.awt.GraphicsEnvironment;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.graphics2d.svg.SVGGraphics2D;
import org.jfree.graphics2d.svg.SVGUtils;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.parameters.SubPlotParameters;
import ohd.hseb.charter.parameters.SubtitleParameters;
import ohd.hseb.charter.parameters.ThresholdParameters;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.OutputTypeSelection;
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
     * Overrides the appearance of the chart to support comparisons between SVG and image outputs across platforms.  
     * 
     * @param engine The charting engine to modify.  This is modified in place.
     * @throws CouldNotLoadRequiredFontException If the liberation font, captured in LiberationSans-Regular.ttf, which should be on the classpath when this is executed, cannot be found.
     * @throws IOException The font file cannot be opened and used to create a font for whatever reason.
     * @throws FontFormatException The format of the font information cannot be understood.
     */
    static void prepareChartEngineForWriting(ChartEngine engine) throws CouldNotLoadRequiredFontException, IOException, FontFormatException
    {
        //#81628 change.  Create the chart and force it to use a Liberation Sans font.  Below is a test run.
        //It will need to be enhanced later to modify *all* fonts in the JFreeChart.
        // Create the chart
        String fontResource = "LiberationSans-Regular.ttf";
        URL fontUrl = GraphicsWriter.class.getClassLoader().getResource( fontResource );

        // Load the font and force it into the chart.
        if ( Objects.isNull( fontUrl ) )
        {
           throw new CouldNotLoadRequiredFontException( "Could not find the " + fontResource + " file on the class path." );
        }

        try ( InputStream fontStream = fontUrl.openStream() )
        {
            Font font = Font.createFont( Font.TRUETYPE_FONT, fontStream ).deriveFont(10.0f);

            // Register font with graphics env
            GraphicsEnvironment graphics = GraphicsEnvironment.getLocalGraphicsEnvironment();
            graphics.registerFont( font );
            
            //Set all ChartEngine fonts to be the liberation font with size 10.
            engine.getChartParameters().getPlotTitle().setFont( font );
            engine.getChartParameters().getLegend().getLegendTitle().setFont( font );
            engine.getChartParameters().getLegend().getLegendEntryFont().setFont( font );
            engine.getChartParameters().getDomainAxis().getLabel().setFont( font );  //One shared domain axis.
            for (SubPlotParameters subPlot : engine.getChartParameters().getSubPlotParameters()) //Range axes by subplot.
            {
                subPlot.getLeftAxis().getLabel().setFont( font ); //Font is used for axis label and tick marks.
                subPlot.getRightAxis().getLabel().setFont( font ); //Font is used for axis label and ticks marks.
            }
            for ( SubtitleParameters parms : engine.getChartParameters().getSubtitleList().getSubtitleList() )
            {
                parms.setFont( font );
            }
            for (ThresholdParameters parms : engine.getChartParameters().getThresholdList().getThresholdParametersList() )
            {
                parms.getLabel().setFont( font );
            }
        }
    }

    /**
     * Writes an output chart to a specified path.
     *
     * @param outputImage the path to the output image
     * @param engine the chart engine.  Note that the method {@link #prepareChartEngineForWriting(ChartEngine)} will modify the drawing parameters of this chart engine in place.
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
            prepareChartEngineForWriting( engine );
            JFreeChart chart = engine.buildChart();
            Path resolvedPath = outputImage;

            // Default is png
            if ( dest.getType() == DestinationType.GRAPHIC || dest.getType() == DestinationType.PNG )
            {
                resolvedPath = resolvedPath.resolveSibling( resolvedPath.getFileName() + ".png" );
                File outputImageFile = resolvedPath.toFile();

                // #58735-18
                ChartUtilities.saveChartAsPNG( outputImageFile, chart /*engine.buildChart() - commented for #81628 change, above*/, width, height );
            }
            else if ( dest.getType() == DestinationType.SVG )
            {
                resolvedPath = resolvedPath.resolveSibling( resolvedPath.getFileName() + ".svg" );
                File outputImageFile = resolvedPath.toFile();
                
                // Create the chart -- Commented out for change related to #81628, above.
                //JFreeChart chart = engine.buildChart();
                
                // Create the svg string
                SVGGraphics2D svg2d = new SVGGraphics2D( width, height );
                // Need to set this to a fixed value as it will otherwise use the system time in nanos, preventing
                // automated testing. #81628-21.
                svg2d.setDefsKeyPrefix( "4744385419576815639" );
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
        catch ( IOException | ChartEngineException | XYChartDataSourceException | CouldNotLoadRequiredFontException | FontFormatException e )
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
            String innerGraphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );

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
