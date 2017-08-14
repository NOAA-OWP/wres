package wres.vis;

import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;

public class ReliabilityDiagramSampleSizeXYChartDataSource extends WRESXYChartDataSource<MetricOutputMapByLeadThreshold<MultiVectorOutput>>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ReliabilityDiagramSampleSizeXYChartDataSource(final int orderIndex,
                                                        final MetricOutputMapByLeadThreshold<MultiVectorOutput> input)
    {
        super(orderIndex, input, input.keySetByThreshold().size());

        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex(1);
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("Forecast Probability");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("Samples");
    }

    @Override
    protected ReliabilityDiagramSampleSizeXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ReliabilityDiagramSampleSizeXYChartDataSource(getDataSourceOrderIndex(), getInput());
    }

    @Override
    protected ReliabilityDiagramSampleSizeXYDataset instantiateXYDataset()
    {
        return new ReliabilityDiagramSampleSizeXYDataset(getInput());
    }

}
