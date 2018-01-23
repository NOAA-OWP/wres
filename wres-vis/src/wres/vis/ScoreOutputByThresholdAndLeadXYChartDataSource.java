package wres.vis;

import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;

/**
 * A chart data source for a single verification metric, indexed by threshold and forecast lead time.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ScoreOutputByThresholdAndLeadXYChartDataSource
        extends WRESXYChartDataSource<MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>>
{
    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ScoreOutputByThresholdAndLeadXYChartDataSource( final int orderIndex,
                                                            final MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> input )
    {
        super( orderIndex, input, input.keySetByTime().size() );

        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "THRESHOLD VALUE@inputUnitsLabelSuffix@" );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );
        WRESTools.applyDefaultJFreeChartColorSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
        WRESTools.applyDefaultJFreeChartShapeSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
    }

    @Override
    protected ScoreOutputByThresholdAndLeadXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ScoreOutputByThresholdAndLeadXYChartDataSource( getDataSourceOrderIndex(), getInput() );
    }

    @Override
    protected ScoreOutputByThresholdAndLeadXYDataset instantiateXYDataset()
    {
        return new ScoreOutputByThresholdAndLeadXYDataset( getInput() );
    }
}
