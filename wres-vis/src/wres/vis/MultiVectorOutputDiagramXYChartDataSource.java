package wres.vis;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMapByLeadThreshold;
import wres.datamodel.MultiVectorOutput;

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
     * @param xConstant the first metric identifier
     * @param yConstant the second metric identifier
     * @param domainTitle the domain axis title
     * @param rangeTitle the range axis title
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


    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     * @param xConstant the first metric identifier
     * @param yConstant the second metric identifier
     * @param domainTitle the domain axis title
     * @param rangeTitle the range axis title
     * @param subPlotIndex the index for the sub-plot
     */
    public MultiVectorOutputDiagramXYChartDataSource(final int orderIndex,
                                               final MetricOutputMapByLeadThreshold<MultiVectorOutput> input,
                                               final MetricConstants xConstant,
                                               final MetricConstants yConstant,
                                               final String domainTitle,
                                               final String rangeTitle,
                                               final int subPlotIndex)
    {
        this(orderIndex, input, xConstant, yConstant, domainTitle, rangeTitle);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex(subPlotIndex);
    }
    
    protected MetricConstants getXConstant()
    {
        return xConstant;
    }
    
    protected MetricConstants getYConstant()
    {
        return yConstant;
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
