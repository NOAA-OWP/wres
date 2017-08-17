package wres.vis;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;

/**
 * A chart data source for the reliability portion (top subplot) of a reliability diagram for which a diagram for each
 * threshold is displayed for a single lead time.
 * 
 * @author hank.herr@***REMOVED***
 * @version 0.1
 * @since 0.1
 */
public class MultiVectorOutputDiagramXYChartDataSource
extends
    WRESXYChartDataSource<MetricOutputMapByLeadThreshold<MultiVectorOutput>>
{
    private final MetricConstants xConstant;
    private final MetricConstants yConstant;

    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public MultiVectorOutputDiagramXYChartDataSource(final int orderIndex,
                                               final MetricOutputMapByLeadThreshold<MultiVectorOutput> input,
                                               final MetricConstants xConstant,
                                               final MetricConstants yConstant,
                                               final String domainTitle,
                                               final String rangeTitle)
    {
        super(orderIndex, input, input.keySet().size());

        this.xConstant=xConstant;
        this.yConstant=yConstant;
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle(domainTitle);//("Forecast Probability");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle(rangeTitle);//("Observed Probability Given Forecast Probability");
        WRESTools.applyDefaultJFreeChartColorSequence(getDefaultFullySpecifiedDataSourceDrawingParameters());
    }

    @Override
    protected MultiVectorOutputDiagramXYChartDataSource instantiateCopyOfDataSource()
    {
        return new MultiVectorOutputDiagramXYChartDataSource(getDataSourceOrderIndex(),
                                                       getInput(),
                                                       xConstant,
                                                       yConstant,
                                                       getDefaultFullySpecifiedDataSourceDrawingParameters().getDefaultDomainAxisTitle(),
                                                       getDefaultFullySpecifiedDataSourceDrawingParameters().getDefaultRangeAxisTitle());
    }

    @Override
    protected MultiVectorOutputDiagramXYDataset instantiateXYDataset()
    {
        return new MultiVectorOutputDiagramXYDataset(getInput(),
                                                     xConstant,
                                                     yConstant);
    }
}
