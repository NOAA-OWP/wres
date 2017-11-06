package wres.vis;

import ohd.hseb.charter.datasource.XYChartDataSource;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.SingleValuedPairs;

/**
 * An {@link XYChartDataSource} that makes use of a {@link MetricInput}.
 * 
 * @author Hank.Herr
 */
public class SingleValuedPairsXYChartDataSource extends WRESXYChartDataSource<SingleValuedPairs>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public SingleValuedPairsXYChartDataSource(final int orderIndex, final SingleValuedPairs input)
    {
        super(orderIndex, input, 1);

        //TODO Need to ensure that these arguments are available here.  How best to do so?
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle("@domainAxisLabelPrefix@@inputUnitsText@");
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle("@rangeAxisLabelPrefix@@inputUnitsText@");
    }

    @Override
    protected SingleValuedPairsXYChartDataSource instantiateCopyOfDataSource()
    {
        return new SingleValuedPairsXYChartDataSource(getDataSourceOrderIndex(), getInput());
    }

    @Override
    protected SingleValuedPairsXYDataset instantiateXYDataset()
    {
        return new SingleValuedPairsXYDataset(getInput());
    }
}
