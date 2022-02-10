package wres.vis;

import java.awt.Color;
import java.awt.Paint;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import org.jfree.chart.plot.DefaultDrawingSupplier;

public class GraphicsUtils
{

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
     * Hidden constructor.
     */
    private GraphicsUtils()
    {
    }

}
