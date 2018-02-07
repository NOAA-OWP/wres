package wres.vis;

import java.util.Objects;

import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.IntervalXYDataset;
import org.jfree.data.xy.XYDataset;

import com.google.common.base.Strings;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.datasource.DefaultXYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;

/**
 * Superclass of {@link DefaultXYChartDataSource} used in wres-vis. It provides a base constructor and methods to build
 * initial parameters, build the {@link XYDataset} need by {@link JFreeChart} and copying the datasource with initial
 * parameters.<br>
 * <br>
 * Instructions for implementing a subclass of {@link WRESXYChartDataSource} are as follows (recommend using another
 * subclass as a starting point and modifying it as needed):<br>
 * <br>
 * 1. Define the generic types appropriately; see parameter descriptions.<br>
 * <br>
 * 2. Override the method {@link #instantiateCopyOfDataSource()} to initialize a new instance data source; see the
 * method.<br>
 * <br>
 * 3. Override the method {@link #instantiateXYDataset()} to instantiate an instance of the
 * {@link WRESAbstractXYDataset}.<br>
 * <br>
 * 4. Create a constructor that calls {@link WRESXYChartDataSource#WRESXYChartDataSource(int, Object, int)} and then
 * overrides any parameters within {@link #getDefaultFullySpecifiedDataSourceDrawingParameters()} appropriately.
 * 
 * @author Hank.Herr
 * @param <T> The generic type of the {@link #input} specifying the data to plot.
 */
public abstract class WRESXYChartDataSource<T> extends DefaultXYChartDataSource
{

    private final T input;

    /**
     * See javadoc of {@link #buildInitialParameters(int, int)}. This will call that method to initialize parameters. Be
     * sure to override any settings as needed, particularly the axis titles which are only filler. Furthermore, this
     * constructor will also set the x-axis type and computed (range) data type to numerical; override if necessary
     * (though it shouldn't be).
     * 
     * @param orderIndex The data source order index.
     * @param input The input providing data to plot.
     * @param numberOfSeries The number of series that will be plotted. A subclass should determine this from the input
     *            argument when calling.
     */
    protected WRESXYChartDataSource(final int orderIndex, final T input, final int numberOfSeries)
    {
        Objects.requireNonNull(input, "Specify a non-null input dataset for building the chart data source.");
        this.input = input;
        buildInitialParameters(orderIndex, numberOfSeries);
        this.setXAxisType(ChartConstants.AXIS_IS_NUMERICAL);
        this.setComputedDataType(ChartConstants.AXIS_IS_NUMERICAL);
    }

    /**
     * Called during construction, this sets the data source drawing index, the plotter to a line-and-scatter, sub plot
     * index to 0, y-axis index to 0, filler for the domain and range axis title for this data source, and initializes
     * series drawing parameters for all series with an empty (non-null) legend name.
     * 
     * @param dataSourceOrderIndex The order index of this data source (presumably specified as argument to the
     *            constructor of this data source).
     * @param numberOfSeries The number of series that will be plotted, which is necessary in order to identify how many
     *            series drawing parameters to initialize.
     */
    private void buildInitialParameters(final int dataSourceOrderIndex, final int numberOfSeries)
    {
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDataSourceOrderIndex(dataSourceOrderIndex);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName("LineAndScatter");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex(0);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setYAxisIndex(0);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("INSERT DEFAULT DOMAIN AXIS TITLE HERE!");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("INSERT DEFAULT RANGE AXIS TITLE HERE!");

        constructAllSeriesDrawingParameters(numberOfSeries);
        for(int i = 0; i < numberOfSeries; i++)
        {
            final SeriesDrawingParameters seriesParms =
                                                      getDefaultFullySpecifiedDataSourceDrawingParameters().getSeriesDrawingParametersForSeriesIndex(i);
            seriesParms.setupDefaultParameters();
            seriesParms.setNameInLegend("");
        }
    }

    protected T getInput()
    {
        return input;
    }

    /**
     * @return A new instance of this {@link WRESXYChartDataSource} subclass. Note that
     *         {@link #returnNewInstanceWithCopyOfInitialParameters()} will call this method and then call
     *         {@link #copyTheseParametersIntoDataSource(DefaultXYChartDataSource)} in order to copy over parameters.
     *         Hence, the subclass does not need to worry about calling that method to copy over other parts of the data
     *         source.
     */
    protected abstract WRESXYChartDataSource<T> instantiateCopyOfDataSource();

    /**
     * @return An instance of {@link WRESAbstractXYDataset} to use in building the {@link JFreeChart}.
     */
    protected abstract IntervalXYDataset instantiateXYDataset();
    
    @Override
    public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
    {
        final WRESXYChartDataSource<T> copy = instantiateCopyOfDataSource();
        copy.copyTheseParametersIntoDataSource(this);
        return copy;
    }

    @Override
    protected XYDataset buildXYDataset(final DataSourceDrawingParameters parameters) throws XYChartDataSourceException
    {
        final IntervalXYDataset dataSet = instantiateXYDataset();

        if ( dataSet instanceof WRESAbstractXYDataset )
        {
            //Set the legend names based on the passed in parameters, which are fully processed.
            //Legend names are set in the dataSet itself, which is why this must be done when the dataSet is created.
            //I know... I don't like it either.
            for ( int i = 0; i < dataSet.getSeriesCount(); i++ )
            {
                if ( !Strings.isNullOrEmpty( parameters.getSeriesDrawingParametersForSeriesIndex( i )
                                                       .getNameInLegend() ) )
                {
                    ( (WRESAbstractXYDataset<?, ?>) dataSet ).setOverrideLegendName( i,
                                                                                     parameters.getArguments()
                                                                                               .replaceArgumentsInString( parameters.getSeriesDrawingParametersForSeriesIndex( i )
                                                                                                                                    .getArgumentReplacedNameInLegend() ) );
                }
            }
        }
        return dataSet;
    }    
}
