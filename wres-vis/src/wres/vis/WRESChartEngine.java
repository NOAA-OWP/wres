package wres.vis;

import java.awt.BasicStroke;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jfree.chart.JFreeChart;
import org.jfree.ui.RectangleInsets;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.parameters.ChartDrawingParameters;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;
import ohd.hseb.charter.parameters.SubPlotParameters;
import ohd.hseb.hefs.utils.arguments.ArgumentsProcessor;

/**
 * Wrapper on {@link ChartEngine} that overrides the {@link ChartEngine#buildChart()} method in order to add diagonals
 * to subplots and square axes.
 * 
 * @author Hank.Herr
 */
class WRESChartEngine extends ChartEngine
{

    private final Map<Integer, SeriesDrawingParameters> subplotIndexToParameters = new HashMap<>();
    private final String axisToSquareAgainstDomain;

    /**
     * @param sources The sources for the chart.
     * @param arguments The arguments.
     * @param defaultParameters Loaded default chart drawing parameters.
     * @param diagonalDataSourceIndices Array of indices indicating the data sources which are actually defining
     *            diagonal lines to draw. From these, parameters will be extracted defining the diagonal line
     *            appearance. Note that such parameters must be defined for the series index -1 (i.e., the default
     *            defining all series index).
     * @param axisToSquareAgainstDomain A string indicating the axes to square. This should be either "left" or "right".
     * @throws ChartEngineException if the {@link ChartEngine} could not be constructed
     */
    WRESChartEngine( final List<XYChartDataSource> sources,
                     final ArgumentsProcessor arguments,
                     final ChartDrawingParameters defaultParameters,
                     final int[] diagonalDataSourceIndices,
                     final String axisToSquareAgainstDomain )
            throws ChartEngineException
    {
        super( arguments, sources, defaultParameters );

        //Determine the series drawing parameters for diagonal lines, but only if the indices are provided.
        if ( diagonalDataSourceIndices != null )
        {
            //For each data source index...
            for ( final int diagSourceIndex : diagonalDataSourceIndices )
            {
                setupSeriesDrawingParametersForDiagonal( defaultParameters, diagSourceIndex );
            }
        }
        
        this.axisToSquareAgainstDomain = axisToSquareAgainstDomain;
    }

    /**
     * Adds series drawing parameter for a diagonal series.
     * @param defaultParameters
     * @param diagSourceIndex Index of the diagonal source with the data source parameters.
     */
    private void setupSeriesDrawingParametersForDiagonal( ChartDrawingParameters defaultParameters,
                                                          int diagSourceIndex )
    {
        //Setup the default drawing parameters to use as a basis.  Note that the parameters will be stored
        //in the default-defining series parameters (those with series index -1).
        final DataSourceDrawingParameters diagonalDrawingParameters =
                new DataSourceDrawingParameters( diagSourceIndex );
        diagonalDrawingParameters.setupDefaultParameters();
        diagonalDrawingParameters.addDefaultDefiningSeriesParameters();
        diagonalDrawingParameters.getDefaultDefiningSeriesParameters().setupDefaultParameters();

        //Copy in defaults and overrides.  The default defining series is what matters!
        if ( defaultParameters != null )
        {
            final DataSourceDrawingParameters defaultParms =
                    defaultParameters.getDataSourceParameters( diagSourceIndex );
            if ( defaultParms != null )
            {
                diagonalDrawingParameters.copyOverriddenParameters( defaultParms );
            }
        }

        //Pull out the default drawing parameters defined for each data source, which are those with series index -1.
        final SeriesDrawingParameters diagDrawingParms =
                diagonalDrawingParameters.getDefaultDefiningSeriesParameters();

        //The subplot on which to plot the diagonal is also defined.
        final int subPlotIndex = diagonalDrawingParameters.getSubPlotIndex();

        //Store what was found.
        subplotIndexToParameters.put( subPlotIndex, diagDrawingParms );
    }

    @Override
    public JFreeChart buildChart() throws ChartEngineException, XYChartDataSourceException
    {
        final JFreeChart chart = super.buildChart();

        //Add diagonal lines to indicated subplots using default appearance.
        for ( final Map.Entry<Integer, SeriesDrawingParameters> entry : subplotIndexToParameters.entrySet() )
        {
            final SeriesDrawingParameters diagonalDrawingParametrs = entry.getValue();
            final BasicStroke stroke = new BasicStroke( diagonalDrawingParametrs.getLineWidth() );

            //This will draw  to the edge.  If the chart is to be viewed and zoomed, we may want to select arbitrarily
            //larger bounds or redraw the diagonal with every chart resize.
            ChartTools.addDiagonalLineToEdge( chart, entry.getKey(), stroke, diagonalDrawingParametrs.getLineColor() );
        }

        //square the axes.
        if ( axisToSquareAgainstDomain != null )
        {
            ChartTools.squareAxes( chart, axisToSquareAgainstDomain );
        }

        //#55497: Add padding to the right side of necessary.  
        boolean visible = false;
        for ( SubPlotParameters parms : getChartParameters().getSubPlotParameters() )
        {
            visible = visible || parms.getRightAxis().getVisible();
        }
        if (!visible)
        {
            chart.setPadding( new RectangleInsets( 0, 0, 0, 40 ) );
        }
        
        return chart;
    }

}
