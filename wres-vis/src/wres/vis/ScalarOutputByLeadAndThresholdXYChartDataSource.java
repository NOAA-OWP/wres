package wres.vis;

import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScalarOutput;

/**
 * A chart data source for a single verification metric, indexed by forecast lead time and threshold.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ScalarOutputByLeadAndThresholdXYChartDataSource
        extends WRESXYChartDataSource<MetricOutputMapByTimeAndThreshold<ScalarOutput>>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ScalarOutputByLeadAndThresholdXYChartDataSource( final int orderIndex,
                                                            final MetricOutputMapByTimeAndThreshold<ScalarOutput> input )
    {
        super( orderIndex, input, input.keySetByThreshold().size() );

        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "FORECAST LEAD TIME [HOUR]" );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );
        WRESTools.applyDefaultJFreeChartColorSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
        WRESTools.applyDefaultJFreeChartShapeSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
    }

    @Override
    protected ScalarOutputByLeadAndThresholdXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ScalarOutputByLeadAndThresholdXYChartDataSource( getDataSourceOrderIndex(), getInput() );
    }

    @Override
    protected ScalarOutputByLeadAndThresholdXYDataset instantiateXYDataset()
    {
        return new ScalarOutputByLeadAndThresholdXYDataset( getInput() );
    }
}
