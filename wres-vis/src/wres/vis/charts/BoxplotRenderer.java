package wres.vis.charts;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;

import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CrosshairState;
import org.jfree.chart.plot.PlotRenderingInfo;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.AbstractXYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRendererState;
import org.jfree.data.xy.XYDataset;

/**
* A renderer that draws box plot items on an {@link org.jfree.chart.plot.XYPlot}.
* 
* @author James Brown
*/

class BoxplotRenderer extends AbstractXYItemRenderer
{
    /**
     * Serial version identifier.
     */

    private static final long serialVersionUID = 255814654677603115L;

    /**
     * Draws a single data item.
     *
     * @param g2 the graphics device
     * @param state the renderer state
     * @param dataArea the area within which the plot is being drawn
     * @param info collects info about the drawing
     * @param plot the plot (can be used to obtain standard color information etc)
     * @param domainAxis the domain axis
     * @param rangeAxis the range axis
     * @param dataset the dataset
     * @param series the series index (zero-based)
     * @param item the item index (zero-based)
     * @param crosshairState crosshair information for the plot (<code>null</code> permitted)
     * @param pass the pass index
     */
    @Override
    public void drawItem( Graphics2D g2,
                          XYItemRendererState state,
                          Rectangle2D dataArea,
                          PlotRenderingInfo info,
                          XYPlot plot,
                          ValueAxis domainAxis,
                          ValueAxis rangeAxis,
                          XYDataset dataset,
                          int series,
                          int item,
                          CrosshairState crosshairState,
                          int pass )
    {
        this.drawVerticalItem( g2, dataArea, plot, domainAxis, rangeAxis, series, item );
    }

    /**
     * Draws a single data item in the vertical orientation.
     *
     * @param g2 the graphics device
     * @param plotArea the area within which the plot is being drawn
     * @param plot the plot (can be used to obtain standard color information etc)
     * @param domainAxis the domain axis
     * @param rangeAxis the range axis
     * @param series the series index (zero-based)
     * @param item the item index (zero-based)
     */
    private void drawVerticalItem( Graphics2D g2,
                                   Rectangle2D plotArea,
                                   XYPlot plot,
                                   ValueAxis domainAxis,
                                   ValueAxis rangeAxis,
                                   int series,
                                   int item )
    {
        XYDataset dataset = plot.getDataset();

        // The x-axis value
        Number x = dataset.getX( series, item );

        // Converts the x-value to pixel location
        double xPixels = domainAxis.valueToJava2D( x.doubleValue(), plotArea, plot.getDomainAxisEdge() );

        int itemCount = dataset.getItemCount( series );
        double boxWidth = this.getBoxWidth( itemCount, plotArea );

        int seriesCount = dataset.getSeriesCount();
        double[] data = new double[seriesCount];
        for ( int i = 0; i < seriesCount; i++ )
        {
            data[i] = dataset.getYValue( i, item );
        }

        // Determine the stroke using the line width.
        Stroke s = this.getItemStroke( series, item );
        if ( s != null )
        {
            g2.setStroke( s );
        }

        // Draw the box

        // Only draw if there are at least 4 items
        int boxLowerBoundIndex = -1;
        int boxUpperBoundIndex = -1;
        if ( data.length > 3 )
        {
            boxLowerBoundIndex = (int) Math.round( 0.2 * ( data.length - 1 ) ); //20% 
            boxUpperBoundIndex = (int) Math.round( 0.8 * ( data.length - 1 ) ); //80%

            // Size and position the box
            double yLower = data[boxLowerBoundIndex]; //used to use bBound
            double yUpper = data[boxUpperBoundIndex]; //used to use tBound

            // Only draw the box if it is defined
            if ( !Double.isNaN( yLower ) && !Double.isNaN( yUpper ) )
            {
                double yyLower = rangeAxis.valueToJava2D( yLower, plotArea, plot.getRangeAxisEdge() );
                double yyUpper = rangeAxis.valueToJava2D( yUpper, plotArea, plot.getRangeAxisEdge() );

                Shape box =
                        new Rectangle2D.Double( xPixels - boxWidth / 2,
                                                yyUpper,
                                                boxWidth,
                                                Math.abs( yyUpper - yyLower ) );

                // Draw the box in the color of the fill
                g2.setPaint( this.getDefaultFillPaint() );
                g2.draw( box );
                g2.fill( box );
            }
        }

        // Draw the whiskers

        // Determine color for line drawing
        Paint p = this.getDefaultPaint();
        if ( p != null )
        {
            g2.setPaint( p );
        }

        g2.setPaint( p );

        for ( int i = 0; i < data.length - 1; i++ )
        {
            double yLower = data[i];
            double yUpper = data[i + 1];
            if ( !Double.isNaN( yLower ) && !Double.isNaN( yUpper ) )
            {
                double yyLower = rangeAxis.valueToJava2D( yLower, plotArea, plot.getRangeAxisEdge() );
                double yyUpper = rangeAxis.valueToJava2D( yUpper, plotArea, plot.getRangeAxisEdge() );
                //Horizontal line top
                Shape line1 = new Line2D.Double( xPixels - boxWidth / 2, yyUpper, xPixels + boxWidth / 2, yyUpper );
                //Horizontal line bottom
                Shape line2 = new Line2D.Double( xPixels - boxWidth / 2, yyLower, xPixels + boxWidth / 2, yyLower );

                g2.draw( line1 );
                g2.draw( line2 );

                // The vertical line
                if ( ( boxLowerBoundIndex < 0 ) || ( i < boxLowerBoundIndex ) || ( i >= boxUpperBoundIndex ) )
                {
                    Shape line3 = new Line2D.Double( xPixels, yyLower, xPixels, yyUpper );
                    g2.draw( line3 );
                }
            }
        }

        this.plotCenterLine( g2, plotArea, plot, data, rangeAxis, xPixels, boxWidth );
    }

    /**
     * @param itemCount the box item count
     * @param plotArea the plot area
     * @return the box width
     */

    private double getBoxWidth( int itemCount, Rectangle2D plotArea )
    {
        double dataAreaX = plotArea.getMaxX() - plotArea.getMinX();
        double maxboxPercent = 0.1;

        double maxboxWidth = dataAreaX * maxboxPercent;

        double exactboxWidth = dataAreaX / itemCount * 4.5 / 7;
        double width;
        if ( exactboxWidth < 3 )
        {
            width = 3;
        }
        else if ( exactboxWidth > maxboxWidth )
        {
            width = maxboxWidth;
        }
        else
        {
            width = exactboxWidth;
        }

        return width;
    }

    /**
     * Plots a center line, typically a median value.
     * @param g2 the graphics context
     * @param plotArea the plot area
     * @param plot the plot
     * @param data the dataset
     * @param rangeAxis the range axis
     * @param xPixels the absolute position in pixels
     * @param boxWidth the width of the box
     */

    private void plotCenterLine( Graphics2D g2,
                                 Rectangle2D plotArea,
                                 XYPlot plot,
                                 double[] data,
                                 ValueAxis rangeAxis,
                                 double xPixels,
                                 double boxWidth )
    {
        if ( data.length > 2 && data.length % 2 != 0 )
        {
            int middle = data.length / 2;
            double cent = data[middle];

            if ( !Double.isNaN( cent ) )
            {
                double cent2 = rangeAxis.valueToJava2D( cent, plotArea, plot.getRangeAxisEdge() );
                Shape line = new Line2D.Double( xPixels - boxWidth / 2, cent2, xPixels + boxWidth / 2, cent2 );
                g2.setPaint( Color.BLACK );
                g2.draw( line );
            }
        }
    }

}
