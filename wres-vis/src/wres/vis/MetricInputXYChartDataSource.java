package wres.vis;

import org.jfree.data.xy.XYDataset;

import ohd.hseb.charter.ChartConstants;
import ohd.hseb.charter.datasource.DefaultXYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSource;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.charter.parameters.DataSourceDrawingParameters;
import ohd.hseb.charter.parameters.SeriesDrawingParameters;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.SingleValuedPairs;

/**
 * An {@link XYChartDataSource} that makes use of a {@link MetricInput}.
 * 
 * @author Hank.Herr
 */
public class MetricInputXYChartDataSource extends DefaultXYChartDataSource
{
    private final SingleValuedPairs metricInput;

    /**
     * @param orderIndex The data source order index within the plotted chart.
     * @param input The {@link MetricInput} for which to display a chart.
     */
    public MetricInputXYChartDataSource(final int orderIndex, final SingleValuedPairs input)
    {
        metricInput = input;
        buildInitialParameters(orderIndex);

        //TODO This stuff needs to come from meta data for MetricInput!!!
        this.setXAxisType(ChartConstants.AXIS_IS_NUMERICAL);
        this.setComputedDataType(ChartConstants.AXIS_IS_NUMERICAL);
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

        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("@domainAxisLabelPrefix@@inputUnitsText@");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("@rangeAxisLabelPrefix@@inputUnitsText@");

        constructAllSeriesDrawingParameters(1);
        final SeriesDrawingParameters seriesParms = this.getDefaultFullySpecifiedDataSourceDrawingParameters()
                                                        .getSeriesDrawingParametersForSeriesIndex(0);
        seriesParms.setupDefaultParameters();

        seriesParms.setNameInLegend("Series");
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
        dataSet = new SingleValuedPairsXYDataset(metricInput);

        //Set the legend names based on the passed in parameters.
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
