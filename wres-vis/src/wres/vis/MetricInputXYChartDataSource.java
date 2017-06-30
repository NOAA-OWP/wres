package wres.vis;

import org.jfree.data.xy.XYDataset;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.datasource.DefaultXYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;
import wres.datamodel.metric.MetricInput;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;

/**
 * An {@link XYChartDataSource} that makes use of a {@link MetricInput}.
 * 
 * @author Hank.Herr
 */
public class MetricInputXYChartDataSource extends DefaultXYChartDataSource
{
    private final MetricInput metricInput;

    /**
     * @param orderIndex The data source order index within the plotted chart.
     * @param input The {@link MetricInput} for which to display a chart.
     */
    public MetricInputXYChartDataSource(final int orderIndex, final MetricInput input)
    {
        metricInput = input;
        buildInitialParameters(orderIndex);

        //TODO This stuff needs to come from meta data for MetricInput!!!
        this.setXAxisType(ChartConstants.AXIS_IS_NUMERICAL);
        this.setComputedDataType(ChartConstants.AXIS_IS_NUMERICAL);
        this.setSourceNameInTable("TEST TABLE NAME");
        this.setUnitsString("TEST UNITS");
        this.setDomainHeader("TEST DOMAIN COL");
    }

    /**
     * Called in the constructor, it calls {@link #constructAllSeriesDrawingParameters(int)} and initializes the data
     * source drawing parameters.
     */
    private void buildInitialParameters(final int dataSourceOrderIndex)
    {
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDataSourceOrderIndex(dataSourceOrderIndex);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setPlotterName("LineAndScatter");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex(0);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setYAxisIndex(0);

        //JB @ 27 June 2017: simplified interface for MetricInput 
        //Initialize the series parameters to be fully defined.
//        constructAllSeriesDrawingParameters(metricInput.size());
//        for(int i = 0; i < metricInput.size(); i++)
//        {
//            final SeriesDrawingParameters seriesParms = this.getDefaultFullySpecifiedDataSourceDrawingParameters()
//                                                            .getSeriesDrawingParametersForSeriesIndex(i);
//
//            //Need fully specified parameters since these are not overrides.  
//            seriesParms.setupDefaultParameters();
//
//            //Override the default legend names in order to number them starting with 0.
//            seriesParms.setNameInLegend("Series " + i);
//            if(i == 0)
//            {
//                seriesParms.setNameInLegend("Series First");
//            }
//        }
        //START: JB @ 27 June 2017: simplified interface for MetricInput
        constructAllSeriesDrawingParameters(1);
        final SeriesDrawingParameters seriesParms = this.getDefaultFullySpecifiedDataSourceDrawingParameters()
        .getSeriesDrawingParametersForSeriesIndex(0);

        //Need fully specified parameters since these are not overrides.  
        seriesParms.setupDefaultParameters();        
        //END: JB
        seriesParms.setNameInLegend("Series First");
    }

    @Override
    public XYChartDataSource returnNewInstanceWithCopyOfInitialParameters() throws XYChartDataSourceException
    {
        final MetricInputXYChartDataSource copy = new MetricInputXYChartDataSource(getDataSourceOrderIndex(),
                                                                                   metricInput);

        copy.copyTheseParametersIntoDataSource(this);
        return copy;
    }

    @Override
    protected XYDataset buildXYDataset(final DataSourceDrawingParameters parameters) throws XYChartDataSourceException
    {
        SingleValuedPairsXYDataset dataSet;

        //Depending on answer to the question at the top of SingleValuedPairs, we may just call 
        //metricInput.buildXYDataset() to acquire the data set and then set the legend names below.
        //Then we wouldn't need this cheezy instanceof if-clause.
        if(metricInput instanceof SingleValuedPairs)
        {
            dataSet = new SingleValuedPairsXYDataset((SingleValuedPairs)metricInput);
        }
        else
        {
            throw new IllegalArgumentException("Class of MetricInput not understood.");
        }

        //Set the legend names based on the passed in override parameters.
        //Legend names are set in the dataSet itself, which is why this must be done when the dataSet is created.
        //I know... I don't like it either.
        for(int i = 0; i < dataSet.getSeriesCount(); i++)
        {
            dataSet.setLegendName(i,
                                  parameters.getArguments()
                                            .replaceArgumentsInString(parameters.getSeriesDrawingParametersForSeriesIndex(i)
                                                                                .getArgumentReplacedNameInLegend()));
        }

        return dataSet;
    }
}
