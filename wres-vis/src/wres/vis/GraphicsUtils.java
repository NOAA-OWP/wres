package wres.vis;

import java.awt.Color;
import java.awt.Paint;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import org.jfree.chart.plot.DefaultDrawingSupplier;

import ohd.hseb.hefs.utils.gui.tools.ColorTools;

public class GraphicsUtils
{

    /** Names of shapes that may be used in point and line charts. */
    public static final String[] SHAPE_NAMES = { "square", "circle", "up triangle", "diamond", "horizontal rectangle",
                                                 "down triangle", "horizontal ellipse", "right triangle",
                                                 "vertical rectangle", "left triangle", "x", "cross" };
    
    /**
     * Retrieves the specified number of time units from the input duration. Accepted units include:
     * 
     * <ol>
     * <li>{@link ChronoUnit#DAYS}</li>
     * <li>{@link ChronoUnit#HOURS}</li>
     * <li>{@link ChronoUnit#MINUTES}</li>
     * <li>{@link ChronoUnit#SECONDS}</li>
     * <li>{@link ChronoUnit#MILLIS}</li>
     * </ol>
     *  
     * @param duration Retrieves the duration
     * @param durationUnits the time units required
     * @return The length of the duration in terms of the project's lead resolution
     * @throws IllegalArgumentException if the durationUnits is not one of the accepted units
     */
    public static long durationToLongUnits( Duration duration, ChronoUnit durationUnits )
    {
        switch ( durationUnits )
        {
            case DAYS:
                return duration.toDays();
            case HOURS:
                return duration.toHours();
            case MINUTES:
                return duration.toMinutes();
            case SECONDS:
                return duration.getSeconds();
            case MILLIS:
                return duration.toMillis();
            default:
                throw new IllegalArgumentException( "The input time units '" + durationUnits
                                                    + "' are not supported "
                                                    + "in this context." );
        }
    }

    /**
     * @return a sequence of base colors.
     */
    
    public static Color[] getColors()
    {
        //Build a list of colors from the JFreeChart defaults and strip out the yellow shades. 
        //Those shades do not show up well on white
        final Paint[] p = DefaultDrawingSupplier.DEFAULT_PAINT_SEQUENCE;
        final ArrayList<Color> baseColors = new ArrayList<>();
        for ( int i = 0; i < p.length; i++ )
        {
            if ( ( (Color) p[i] ).getRed() != 255 || ( (Color) p[i] ).getGreen() != 255 )
            {
                baseColors.add( (Color) p[i] );
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
     * @param numberOfColors the number of colors to create.
     * @param baseColors the colors that dictate the palette.
     * @return a palette of colors.
     */
    public static Color[] getColorPalette( final int numberOfColors, final Color... baseColors )
    {
        final int numberOfBaseColors = baseColors.length;
        final Color[] palette = new Color[numberOfColors];

        //No shade computations needed.
        if ( numberOfColors <= numberOfBaseColors )
        {
            System.arraycopy( baseColors, 0, palette, 0, palette.length );
        }
        else
        {
            int baseColorIndex = 0;
            double baseColorFractionalCounter = 0.0;
            double mixingFraction;

            //This algorithms 'walks' from 0 to the number of base colors such that the number of steps
            //taken equals the number of colors to create.
            for ( int paletteIndex = 0; paletteIndex < palette.length; paletteIndex++ )
            {
                //Base color index is used to identify the first of the two colors.  It is the truncation
                //of the fractional counter.
                baseColorIndex = (int) baseColorFractionalCounter;
                mixingFraction = ( baseColorFractionalCounter - baseColorIndex );
                if ( baseColorIndex == numberOfBaseColors - 1 ) //The last color is called for, which causes an index error
                {
                    baseColorIndex--;
                    mixingFraction = 1.0;
                }

                //Mix them and set the palette color.
                palette[paletteIndex] = ColorTools.mixColors( baseColors[baseColorIndex],
                                                              baseColors[baseColorIndex + 1],
                                                              mixingFraction );

                //Increment the counter.  The step size is the number of total colors minus 1.  This is
                //because if we start counting at 0, the number of steps taken is actually numberOfColors - 1. 
                baseColorFractionalCounter += ( numberOfBaseColors - 1.0 ) / ( numberOfColors - 1.0 );
            }
        }

        return palette;
    } 

    /**
     * Hidden constructor.
     */
    private GraphicsUtils()
    {
    }

}
