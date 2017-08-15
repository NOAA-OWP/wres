package wres.vis;

import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;
/**
 * A chart data source for the reliability portion (top subplot) of a reliability diagram for which 
 * a diagram for each threshold is displayed for a single lead time.
 * 
 * @author hank.herr@***REMOVED***
 * @version 0.1
 * @since 0.1
 */
public class ReliabilityDiagramXYChartDataSource extends WRESXYChartDataSource<MetricOutputMapByLeadThreshold<MultiVectorOutput>>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ReliabilityDiagramXYChartDataSource(final int orderIndex,
                                               final MetricOutputMapByLeadThreshold<MultiVectorOutput> input)
    {
        super(orderIndex, input, input.keySetByThreshold().size());

        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("Forecast Probability");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("Observed Probability Given Forecast Probability");
        WRESTools.applyDefaultJFreeChartColorSequence(getDefaultFullySpecifiedDataSourceDrawingParameters());
    }

    @Override
    protected ReliabilityDiagramXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ReliabilityDiagramXYChartDataSource(getDataSourceOrderIndex(), getInput());
    }

    @Override
    protected ReliabilityDiagramXYDataset instantiateXYDataset()
    {
        return new ReliabilityDiagramXYDataset(getInput());
    }
}
