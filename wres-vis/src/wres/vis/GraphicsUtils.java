package wres.vis;

import java.awt.Color;
import java.awt.Paint;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import org.jfree.chart.plot.DefaultDrawingSupplier;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.hefs.utils.gui.tools.ColorTools;

class GraphicsUtils
{
    
    /**
     * The colors to use.
     */
    
    private final Color[] colors;
    
    /**
     * Uses, as a starting point, {@link DefaultDrawingSupplier#DEFAULT_PAINT_SEQUENCE}.  If that array of colors is no smaller than
     * than the number of series, then it just applies each color to the corresponding series in the provided parameters.
     * Otherwise, it calls {@link ColorTools#buildColorPalette(int, Color...)} to build a list BASED on that list in order to 
     * acquire a sufficient number of colors.  That algorithm is basically an averaging approach.
     * 
     * @param parameters The parameters to which the rotating colors scheme will be applied.
     */
    void applyDefaultJFreeChartColorSequence( final DataSourceDrawingParameters parameters )
    {
        Color[] innerColors = this.getColors();
        
        //Now we are ready to apply the palette.  Note that, if there are not enough colors in the JFreeChart
        //palette after stripping out the yellows, then the ColorTools method will be used with blue,
        //green, and red.
        final int seriesCount = parameters.getSeriesParametersCount();
        if ( innerColors.length < seriesCount )
        {
            innerColors = ColorTools.buildColorPalette( seriesCount, Color.BLUE, Color.GREEN, Color.RED );
        }  
        
        ChartTools.applyRotatingColorSchemeToSeries( innerColors, parameters );
    }

    /**
     * @param parameters The parameters to which the rotating shape scheme will be applied.
     */
    static void applyDefaultJFreeChartShapeSequence( final DataSourceDrawingParameters parameters )
    {
        //Build a list of colors from the JFreeChart defaults and strip out the yellow shades. 
        //Those shades do not show up well on white
        ChartTools.applyRotatingShapeSchemeToSeries( ChartConstants.SHAPE_NAMES, parameters );
    }
    
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
    static long durationToLongUnits( Duration duration, ChronoUnit durationUnits )
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
     * @return the colors
     */
    
    private Color[] getColors()
    {
        return this.colors;
    }
    
    /**
     * Constructs and instance.
     */
    GraphicsUtils()
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

        this.colors = baseColors.toArray( new Color[] {} );  
    }    

}
