package wres.vis.charts;

import java.awt.Color;
import java.awt.Paint;
import java.util.ArrayList;

import org.jfree.chart.plot.DefaultDrawingSupplier;

import wres.statistics.generated.MetricName;

/**
 * A utility class for graphics.
 *
 * @author James Brown
 * @author Hank Herr
 */

public class GraphicsUtils
{
    /** The scenario label to use when plotting a baseline scenario in the same plot as a predicted scenario. */
    public static final String BASELINE_SCENARIO_LABEL = " (dashed)";

    /**
     * @return a sequence of base colors.
     */
    public static Color[] getColors()
    {
        // Build a list of colors from the JFreeChart defaults and strip out the yellow shades.
        // Those shades do not show up well on white
        final Paint[] p = DefaultDrawingSupplier.DEFAULT_PAINT_SEQUENCE;
        final ArrayList<Color> baseColors = new ArrayList<>();
        for ( Paint paint : p )
        {
            Color c = ( Color ) paint;
            if ( c.getRed() != 255 || c.getGreen() != 255 )
            {
                baseColors.add( c );
            }
        }

        return baseColors.toArray( new Color[] {} );
    }

    /**
     * Builds an array of colors, stepping from the first color provided to the last color provided through each of the
     * intermediary colors. This algorithm cannot guarantee that the intermediary colors will be in the list, since the
     * steps are all equal sized, meaning that colors can be stepped over by this algorithm. Only the first and last
     * colors are guaranteed to be in the returned array.
     *
     * @param numberOfColors the number of colors to create
     * @param baseColors the colors that dictate the palette
     * @return a palette of colors
     * @throws IllegalArgumentException if the number of colors is fewer than one or base colors fewer than two
     */
    public static Color[] getColorPalette( final int numberOfColors, final Color... baseColors )
    {
        int numberOfBaseColors = baseColors.length;

        if ( numberOfColors < 1 )
        {
            throw new IllegalArgumentException( "Cannot create a color ramp without one of more colors." );
        }

        if ( numberOfBaseColors < 2 )
        {
            throw new IllegalArgumentException( "Cannot create a color ramp with fewer than two base colors." );
        }

        Color[] palette = new Color[numberOfColors];

        // No shade computations needed.
        if ( numberOfColors <= numberOfBaseColors )
        {
            System.arraycopy( baseColors, 0, palette, 0, palette.length );
        }
        else
        {
            int baseColorIndex;
            double baseColorFractionalCounter = 0.0;
            double mixingFraction;

            // This algorithms 'walks' from 0 to the number of base colors such that the number of steps
            // taken equals the number of colors to create.
            for ( int paletteIndex = 0; paletteIndex < palette.length; paletteIndex++ )
            {
                // Base color index is used to identify the first of the two colors.  It is the truncation
                // of the fractional counter.
                baseColorIndex = ( int ) baseColorFractionalCounter;
                mixingFraction = ( baseColorFractionalCounter - baseColorIndex );
                if ( baseColorIndex
                     == numberOfBaseColors - 1 ) //The last color is called for, which causes an index error
                {
                    baseColorIndex--;
                    mixingFraction = 1.0;
                }

                // Mix them and set the palette color.
                palette[paletteIndex] = GraphicsUtils.mixColors( baseColors[baseColorIndex],
                                                                 baseColors[baseColorIndex + 1],
                                                                 mixingFraction );

                // Increment the counter.  The step size is the number of total colors minus 1.  This is
                // because if we start counting at 0, the number of steps taken is actually numberOfColors - 1. 
                baseColorFractionalCounter += ( numberOfBaseColors - 1.0 ) / ( numberOfColors - 1.0 );
            }
        }

        return palette;
    }

    /**
     * @param name the metric component name to test
     * @return whether the metric component name is a default name
     */
    public static boolean isNotDefaultMetricComponentName( MetricName name )
    {
        return name != MetricName.UNDEFINED
               && name != MetricName.MAIN;
    }

    /**
     * @param first the starting color
     * @param second the ending color
     * @param distanceFraction the fraction (between 0 and 1) of the difference (distance) between the two colors to
     *            travel to create the mixed color. A value of 0.0 returns the first color, and 1.0 returns the second
     * @return a mixed color with the distanceFraction dictating the mix weight
     */
    private static Color mixColors( Color first, Color second, double distanceFraction )
    {
        return new Color( ( int ) ( first.getRed() + distanceFraction * ( second.getRed() - first.getRed() ) ),
                          ( int ) ( first.getGreen() + distanceFraction * ( second.getGreen() - first.getGreen() ) ),
                          ( int ) ( first.getBlue() + distanceFraction * ( second.getBlue() - first.getBlue() ) ),
                          ( int ) ( first.getAlpha() + distanceFraction * ( second.getAlpha() - first.getAlpha() ) ) );
    }

    /**
     * Hidden constructor.
     */
    private GraphicsUtils()
    {
    }
}
