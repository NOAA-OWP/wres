package wres.vis;

import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.ScalarOutput;

/**
 * A chart data source for a single verification metric, indexed by threshold and forecast lead time.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ScalarOutputByThresholdLeadXYChartDataSource extends WRESXYChartDataSource<MetricOutputMapByLeadThreshold<ScalarOutput>>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ScalarOutputByThresholdLeadXYChartDataSource(final int orderIndex,
                                                        final MetricOutputMapByLeadThreshold<ScalarOutput> input)
    {
        super(orderIndex, input, input.keySetByLead().size());

        //TODO Need to ensure that the arguments used below are standard arguments created in the factory that generates images!
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("THRESHOLD VALUE@inputUnitsText@");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("@metricShortName@@outputUnitsText@");
    }

    @Override
    protected ScalarOutputByThresholdLeadXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ScalarOutputByThresholdLeadXYChartDataSource(getDataSourceOrderIndex(), getInput());
    }

    @Override
    protected ScalarOutputByThresholdLeadXYDataset instantiateXYDataset()
    {
        return new ScalarOutputByThresholdLeadXYDataset(getInput());
    }
}
