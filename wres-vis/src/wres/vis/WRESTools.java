package wres.vis;

import java.awt.Color;
import java.awt.Paint;
import java.util.ArrayList;

import org.jfree.chart.plot.DefaultDrawingSupplier;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.hefs.utils.gui.tools.ColorTools;

public class WRESTools
{

    /**
     * Hiding constructor.
     */
    private WRESTools()
    {
        
    }
    
    /**
     * Uses, as a starting point, {@link DefaultDrawingSupplier#DEFAULT_PAINT_SEQUENCE}.  If that array of colors is no smaller than
     * than the number of series, then it just applies each color to the corresponding series in the provided parameters.
     * Otherwise, it calls {@link ColorTools#buildColorPalette(int, Color...)} to build a list BASED on that list in order to 
     * acquire a sufficient number of colors.  That algorithm is basically an averaging approach.
     * 
     * @param parameters The parameters to which the rotating colors scheme will be applied.
     */
    public static void applyDefaultJFreeChartColorSequence(final DataSourceDrawingParameters parameters)
    {
        //Build a list of colors from the JFreeChart defaults and strip out the yellow shades. 
        //Those shades do not show up well on white
        final Paint[] p = DefaultDrawingSupplier.DEFAULT_PAINT_SEQUENCE;
        final ArrayList<Color> baseColors = new ArrayList<>();
        for(int i = 0; i < p.length; i++) 
        {
            if(((Color)p[i]).getRed() != 255 ||((Color)p[i]).getGreen() != 255) 
            {
                baseColors.add((Color)p[i]);
            }
        }
        
        //Now we are ready to apply the palette.  Note that, if there are not enough colors in the JFreeChart
        //palette after stripping out the yellows, then this the ColorTools method will be used with blue,
        //green, and red.
        final int seriesCount = parameters.getSeriesParametersCount();
        Color[] colorsToApply = baseColors.toArray(new Color[]{});
        if (baseColors.size() < seriesCount)
        {
            colorsToApply = ColorTools.buildColorPalette(seriesCount, Color.BLUE, Color.GREEN, Color.RED);
        }
        ChartTools.applyRotatingColorSchemeToSeries(colorsToApply, parameters);
    }

    
    /**
     * @param parameters The parameters to which the rotating shape scheme will be applied.
     */
    public static void applyDefaultJFreeChartShapeSequence(final DataSourceDrawingParameters parameters)
    {
        //Build a list of colors from the JFreeChart defaults and strip out the yellow shades. 
        //Those shades do not show up well on white
        final String[] p = ChartConstants.SHAPE_NAMES;
        ChartTools.applyRotatingShapeSchemeToSeries( p, parameters );
    }
    
}
