package wres.vis.writing;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.imageio.ImageIO;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.svg.SVGGraphics2D;
import org.jfree.svg.SVGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Slicer;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.Statistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.GraphicFormat;
import wres.statistics.generated.Outputs.GraphicFormat.GraphicShape;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.statistics.generated.SummaryStatistic;
import wres.vis.charts.ChartFactory;

/**
 * An abstract base class for writing graphics formats.
 *
 * @author James Brown
 */

abstract class GraphicsWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsWriter.class );

    private static final ChartFactory CHART_FACTORY = ChartFactory.of();

    // Do not use a file cache for image outputs
    static
    {
        LOGGER.debug( "Setting javax.imageio.ImageIO.setUseCache( boolean useCache ) to: {}.", false );

        ImageIO.setUseCache( false );
    }

    /**
     * Default chart height in pixels.
     */

    private static final int DEFAULT_GRAPHIC_HEIGHT = 600;

    /**
     * Default chart width in pixels.
     */

    private static final int DEFAULT_GRAPHIC_WIDTH = 800;

    /**
     * A description of the outputs required.
     */

    private final Outputs outputs;

    /**
     * The output directory to use to write
     */
    private final Path outputDirectory;

    /**
     * Returns the outputs description
     *
     * @return the outputs description
     */

    Outputs getOutputsDescription()
    {
        return this.outputs;
    }

    /**
     * @return the output directory
     */

    Path getOutputDirectory()
    {
        return this.outputDirectory;
    }

    /**
     * Returns a {@link ChartFactory} for creating charts from statistics.
     *
     * @return a chart factory.
     */

    static ChartFactory getChartFactory()
    {
        return GraphicsWriter.CHART_FACTORY;
    }

    /**
     * Writes an output chart to a prescribed set of graphics formats.
     *
     * @param path the path to write, without the image format extension
     * @param chart the chart
     * @param metric the metric whose statistics will be written to a format, unless the metric/format is suppressed
     * @return the path actually written
     * @throws GraphicsWriteException if the chart could not be written
     */

    static Set<Path> writeGraphic( Path path,
                                   JFreeChart chart,
                                   MetricName metric,
                                   Outputs outputs )
    {
        Objects.requireNonNull( path );
        Objects.requireNonNull( chart );
        Objects.requireNonNull( outputs );

        Set<Path> returnMe = new TreeSet<>();
        Path resolvedPath = null;

        try
        {
            // Default is png
            if ( outputs.hasPng()
                 && !outputs.getPng()
                            .getOptions()
                            .getIgnoreList()
                            .contains( metric ) )
            {
                int height = GraphicsWriter.getGraphicHeight( outputs.getPng().getOptions().getHeight() );
                int width = GraphicsWriter.getGraphicWidth( outputs.getPng().getOptions().getWidth() );
                resolvedPath = path.resolveSibling( path.getFileName() + ".png" );

                // Write if the path has not already been written
                if ( GraphicsWriter.validatePath( resolvedPath ) )
                {
                    // Add now to enable clean-up on failure
                    returnMe.add( resolvedPath );

                    File outputImageFile = resolvedPath.toFile();

                    // #58735-18
                    ChartUtils.saveChartAsPNG( outputImageFile, chart, width, height );
                }
            }
            if ( outputs.hasSvg()
                 && !outputs.getSvg()
                            .getOptions()
                            .getIgnoreList()
                            .contains( metric ) )
            {
                int height = GraphicsWriter.getGraphicHeight( outputs.getPng().getOptions().getHeight() );
                int width = GraphicsWriter.getGraphicWidth( outputs.getPng().getOptions().getWidth() );
                resolvedPath = path.resolveSibling( path.getFileName() + ".svg" );

                // Write if the path has not already been written
                if ( GraphicsWriter.validatePath( resolvedPath ) )
                {
                    // Add now to enable clean-up on failure
                    returnMe.add( resolvedPath );

                    File outputImageFile = resolvedPath.toFile();

                    // Create the svg string
                    SVGGraphics2D svg2d = new SVGGraphics2D( width, height );
                    // Need to set this to a fixed value as it will otherwise use the system time in nanos, preventing
                    // automated testing. #81628-21.
                    svg2d.setDefsKeyPrefix( "4744385419576815639" );

                    chart.draw( svg2d, new Rectangle2D.Double( 0, 0, width, height ) );
                    String svgElement = svg2d.getSVGElement();

                    SVGUtils.writeToSVG( outputImageFile, svgElement );
                }
            }

            return Collections.unmodifiableSet( returnMe );
        }
        catch ( IOException | IllegalArgumentException e )
        {
            // Clean up, to allow recovery. See #83816
            GraphicsWriter.deletePaths( returnMe );

            throw new GraphicsWriteException( "Error while writing chart to '" + resolvedPath + "'.", e );
        }
    }

    /**
     * Slices the statistics into two groups, one containing summary statistics the other containing raw statistics.
     * @param statistics the statistics to slice
     * @param qualifier a function that generates a grouping qualifier from a statistic
     * @param allowed the summary statistics allowed in this context
     * @return the grouped statistics
     * @param <T> the type of statistic
     */
    static <T extends Statistic<?>> List<List<T>> groupBySummaryStatistics( List<T> statistics,
                                                                            Function<T, String> qualifier,
                                                                            Set<SummaryStatistic.StatisticName> allowed )
    {
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( allowed );

        LOGGER.debug( "While grouping statistics, the following summary statistics were allowed: {}", allowed );

        List<List<T>> sliced = new ArrayList<>();

        // Slice by summary statistic presence/absence, ignoring resampled quantiles
        Predicate<T> summaryStat
                = s -> s.isSummaryStatistic()
                       && !s.getSummaryStatistic()
                            .getDimensionList()
                            .contains( SummaryStatistic.StatisticDimension.RESAMPLED );

        List<T> summaryStats =
                Slicer.filter( statistics,
                               summaryStat );

        // Partition the summary statistics by statistic name and dimensions
        Map<Pair<Pair<SummaryStatistic.StatisticName, List<SummaryStatistic.StatisticDimension>>, String>, List<T>>
                grouped =
                summaryStats.stream()
                            .filter( s -> allowed.contains( s.getSummaryStatistic()
                                                             .getStatistic() ) )
                            .collect( Collectors.groupingBy( s -> Pair.of( Pair.of( s.getSummaryStatistic()
                                                                                     .getStatistic(),
                                                                                    s.getSummaryStatistic()
                                                                                     .getDimensionList() ),
                                                                           qualifier.apply( s ) ) ) );

        List<T> noSummaryStats =
                Slicer.filter( statistics,
                               summaryStat.negate() );

        if ( !noSummaryStats.isEmpty() )
        {
            sliced.add( noSummaryStats );
        }

        if ( !grouped.isEmpty() )
        {
            sliced.addAll( grouped.values() );
        }

        return Collections.unmodifiableList( sliced );
    }

    /**
     * Returns representative pool metadata from the supplied statistics. When there are statistics present for both
     * main and baseline pairs, returns the metadata for the first main pool encountered, else the first pool
     * encountered.
     *
     * @param statistics the statistics
     * @return the representative metadata
     * @param <T> the statistic type
     */

    static <T extends Statistic<?>> PoolMetadata getPoolMetadata( List<T> statistics )
    {
        boolean baselinePools = statistics.stream()
                                          .anyMatch( s -> s.getPoolMetadata()
                                                           .getPoolDescription()
                                                           .getIsBaselinePool() );

        List<PoolMetadata> notBaselinePools = statistics.stream()
                                                        .map( s -> s.getPoolMetadata() )
                                                        .filter( poolMetadata -> !poolMetadata.getPoolDescription()
                                                                                              .getIsBaselinePool() )
                                                        .toList();

        if ( baselinePools && !notBaselinePools.isEmpty() )
        {
            return notBaselinePools.get( 0 );
        }

        return statistics.get( 0 )
                         .getPoolMetadata();
    }

    /**
     * Helper that groups destinations by their common graphics parameters. Each inner outputs requires one set of 
     * graphics, written for each format present.
     *
     * @param outputs the outputs
     * @return the groups of outputs by common graphics parameters
     */

    static Collection<Outputs> getOutputsGroupedByGraphicsParameters( Outputs outputs )
    {
        Objects.requireNonNull( outputs );

        // If there is only one format requested, then there is only one group
        Collection<Outputs> returnMe = new ArrayList<>();
        if ( outputs.hasPng() && outputs.hasSvg() )
        {
            PngFormat png = outputs.getPng();
            SvgFormat svg = outputs.getSvg();

            // Both have the same graphics parameters, so keep in one outputs
            if ( png.getOptions().equals( svg.getOptions() ) )
            {
                returnMe.add( outputs );
            }
            else
            {
                Outputs pngToAdd = outputs.toBuilder()
                                          .clearSvg()
                                          .build();
                Outputs svgToAdd = outputs.toBuilder()
                                          .clearPng()
                                          .build();
                returnMe.add( pngToAdd );
                returnMe.add( svgToAdd );
            }
        }
        else
        {
            returnMe.add( outputs );
        }

        return Collections.unmodifiableCollection( returnMe );
    }

    /**
     * Uncovers the graphic parameters from a description of the outputs. Assumes that all graphics contain the same
     * graphics declarations. Use {@link GraphicsWriter#getOutputsGroupedByGraphicsParameters(Outputs)} to obtain output 
     * groups.
     *
     * @author James Brown
     */

    static class GraphicsHelper
    {
        /** The shape of graphic. */
        private final GraphicShape graphicShape;

        /** The duration units. */
        private final ChronoUnit durationUnits;

        /**
         * Returns a graphics helper.
         *
         * @param outputs a description of the required outputs
         * @return a graphics helper
         */

        static GraphicsHelper of( Outputs outputs )
        {
            return new GraphicsHelper( outputs );
        }

        /**
         * Builds a helper.
         *
         * @param outputs a description of the required outputs
         * @throws NullPointerException if the outputs is null
         */

        private GraphicsHelper( Outputs outputs )
        {
            Objects.requireNonNull( outputs );

            GraphicFormat graphicsOptions = null;

            if ( outputs.hasPng()
                 && outputs.getPng()
                           .hasOptions() )
            {
                graphicsOptions = outputs.getPng()
                                         .getOptions();
            }
            else if ( outputs.hasSvg()
                      && outputs.getSvg()
                                .hasOptions() )
            {
                graphicsOptions = outputs.getSvg()
                                         .getOptions();
            }

            // Default to global type parameter
            GraphicShape innerGraphicShape = GraphicShape.DEFAULT;
            if ( Objects.nonNull( graphicsOptions ) )
            {
                innerGraphicShape = graphicsOptions.getShape();
            }

            this.graphicShape = innerGraphicShape;
            this.durationUnits = this.getDurationUnitsFromOutputs( outputs );
        }

        /**
         * Uncovers the duration units from an {@link Outputs} message. Throws an exception if more than one duration unit
         * is present. Formats should be written for common graphics parameters. See 
         * {@link GraphicsWriter#getOutputsGroupedByGraphicsParameters(Outputs)}. Returns a default of 
         * {@link ChronoUnit#HOURS} if no units are present.
         *
         * @param outputs the outputs
         * @return the duration units for graphics writing
         * @throws NullPointerException if the input is null
         * @throws IllegalArgumentException if there are multiple duration units 
         */

        private ChronoUnit getDurationUnitsFromOutputs( Outputs outputs )
        {
            Objects.requireNonNull( outputs );

            ChronoUnit returnMe = ChronoUnit.HOURS;

            if ( outputs.hasPng()
                 && outputs.hasSvg()
                 && outputs.getSvg()
                           .hasOptions()
                 && outputs.getPng()
                           .hasOptions()
                 && !outputs.getPng()
                            .getOptions()
                            .getLeadUnit()
                            .equals( outputs.getSvg()
                                            .getOptions()
                                            .getLeadUnit() ) )
            {
                throw new IllegalArgumentException( "Discovered more than one lead duration unit in the outputs "
                                                    + "message ("
                                                    + outputs.getPng().getOptions().getLeadUnit()
                                                    + ", "
                                                    + outputs.getSvg().getOptions().getLeadUnit()
                                                    + ")." );
            }

            if ( outputs.hasPng() )
            {
                returnMe = ChronoUnit.valueOf( outputs.getPng()
                                                      .getOptions()
                                                      .getLeadUnit()
                                                      .name() );
            }
            else if ( outputs.hasSvg() )
            {
                returnMe = ChronoUnit.valueOf( outputs.getSvg()
                                                      .getOptions()
                                                      .getLeadUnit()
                                                      .name() );
            }

            return returnMe;
        }

        /**
         * @return the shape of graphic.
         */

        GraphicShape getGraphicShape()
        {
            return this.graphicShape;
        }

        /**
         * @return the duration units.
         */

        ChronoUnit getDurationUnits()
        {
            return this.durationUnits;
        }
    }

    /**
     * Hidden constructor.
     *
     * @param outputs a description of the required outputs
     * @param outputDirectory the directory into which to write
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the path is not a writable directory
     */

    GraphicsWriter( Outputs outputs,
                    Path outputDirectory )
    {
        Objects.requireNonNull( outputs, "Specify a non-null outputs description." );
        Objects.requireNonNull( outputDirectory, "Specify non-null output directory." );

        LOGGER.debug( "Creating a graphics format writer." );

        // Validate
        if ( !Files.isDirectory( outputDirectory ) || !Files.exists( outputDirectory )
             || !Files.isWritable( outputDirectory ) )
        {
            throw new IllegalArgumentException( "Cannot create a graphics writer because the path '" + outputDirectory
                                                + "' is not an existing, writable directory." );
        }

        this.outputs = outputs;
        this.outputDirectory = outputDirectory;

        LOGGER.debug( "Created a graphics format writer." );
    }

    /**
     * Validates that the file object represented by the path does not already exist.
     *
     * @return true if the path is valid to write, false if it exists and is, therefore, invalid
     */

    private static boolean validatePath( Path path )
    {
        boolean fileExists = Files.exists( path );

        // #81735-173 and #86077
        if ( fileExists && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Cannot write to path {} because it already exists. This may occur when retrying several "
                         + "format writers of which only some failed previously, but is otherwise unexpected behavior "
                         + "that may indicate an error in format writing. The file has been retained and not modified.",
                         path );
        }

        return !fileExists;
    }

    /**
     * Attempts to delete a set of paths on encountering an error.
     *
     * @param pathsToDelete the paths to delete
     */

    private static void deletePaths( Set<Path> pathsToDelete )
    {
        // Clean up. This should happen anyway, but is essential for the writer to be "retry friendly" when the
        // failure to write is recoverable
        LOGGER.debug( "Deleting the following paths that were created before an exception was encountered in the "
                      + "writer: {}.",
                      pathsToDelete );

        for ( Path nextPath : pathsToDelete )
        {
            try
            {
                Files.deleteIfExists( nextPath );
            }
            catch ( IOException f )
            {
                LOGGER.error( "Failed to delete a path created before an exception was encountered: {}.",
                              nextPath );
            }
        }
    }

    /**
     * @param height the height
     * @return the height if the height is greater than zero, else the {@link GraphicsWriter#DEFAULT_GRAPHIC_HEIGHT}.
     */

    private static int getGraphicHeight( int height )
    {
        if ( height > 0 )
        {
            return height;
        }

        return GraphicsWriter.DEFAULT_GRAPHIC_HEIGHT;
    }

    /**
     * @param width the height
     * @return the width if the width is greater than zero, else the {@link GraphicsWriter#DEFAULT_GRAPHIC_WIDTH}.
     */

    private static int getGraphicWidth( int width )
    {
        if ( width > 0 )
        {
            return width;
        }

        return GraphicsWriter.DEFAULT_GRAPHIC_WIDTH;
    }
}
