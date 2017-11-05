package wres.vis;

import wres.datamodel.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.ScalarOutput;

/**
 * A chart data source for a single verification metric, indexed by forecast lead time and threshold.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ScalarOutputByLeadThresholdXYChartDataSource extends WRESXYChartDataSource<MetricOutputMapByTimeAndThreshold<ScalarOutput>>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ScalarOutputByLeadThresholdXYChartDataSource(final int orderIndex,
                                                        final MetricOutputMapByTimeAndThreshold<ScalarOutput> input)
    {
        super(orderIndex, input, input.keySetByThreshold().size());

        //TODO Must make use of arguments that are provided by default.  How can I best ensure this?
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("FORECAST LEAD TIME [HOUR]");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("@metricShortName@@metricComponentNameSuffix@@outputUnitsText@");
        WRESTools.applyDefaultJFreeChartColorSequence(getDefaultFullySpecifiedDataSourceDrawingParameters());
    }

    @Override
    protected ScalarOutputByLeadThresholdXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ScalarOutputByLeadThresholdXYChartDataSource(getDataSourceOrderIndex(), getInput());
    }

    @Override
    protected ScalarOutputByLeadThresholdXYDataset instantiateXYDataset()
    {
        return new ScalarOutputByLeadThresholdXYDataset(getInput());
    }
}
