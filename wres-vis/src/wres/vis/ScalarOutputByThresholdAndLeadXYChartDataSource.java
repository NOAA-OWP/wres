package wres.vis;

import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScalarOutput;

/**
 * A chart data source for a single verification metric, indexed by threshold and forecast lead time.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ScalarOutputByThresholdAndLeadXYChartDataSource extends WRESXYChartDataSource<MetricOutputMapByTimeAndThreshold<ScalarOutput>>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ScalarOutputByThresholdAndLeadXYChartDataSource(final int orderIndex,
                                                        final MetricOutputMapByTimeAndThreshold<ScalarOutput> input)
    {
        super(orderIndex, input, input.keySetByTime().size());

        //TODO Need to ensure that the arguments used below are standard arguments created in the factory that generates images!
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("THRESHOLD VALUE@inputUnitsText@");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("@metricShortName@@metricComponentNameSuffix@@outputUnitsText@");
        WRESTools.applyDefaultJFreeChartColorSequence(getDefaultFullySpecifiedDataSourceDrawingParameters());
    }

    @Override
    protected ScalarOutputByThresholdAndLeadXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ScalarOutputByThresholdAndLeadXYChartDataSource(getDataSourceOrderIndex(), getInput());
    }

    @Override
    protected ScalarOutputByThresholdAndLeadXYDataset instantiateXYDataset()
    {
        return new ScalarOutputByThresholdAndLeadXYDataset(getInput());
    }
}
