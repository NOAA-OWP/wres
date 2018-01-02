package wres.vis;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.IntervalXYDataset;

import ohd.hseb.charter.ChartConstants;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScalarOutput;

/**
 * A chart data source for a {@link ScalarOutput} plotted by {@link TimeWindow#getMidPointTime()} on the 
 * domain axis, with individual series for each lead time and threshold.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ScalarOutputByPoolingWindowXYChartDataSource
        extends WRESXYChartDataSource<MetricOutputMapByTimeAndThreshold<ScalarOutput>>
{

    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public ScalarOutputByPoolingWindowXYChartDataSource( final int orderIndex,
                                                         final MetricOutputMapByTimeAndThreshold<ScalarOutput> input )
    {
        super( orderIndex, input, input.keySetByThreshold().size() * input.keySetByLeadTimeInHours().size() );

        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "TIME AT CENTER OF WINDOW [UTC]" );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );
        WRESTools.applyDefaultJFreeChartColorSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
        WRESTools.applyDefaultJFreeChartShapeSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
    }

    @Override
    protected ScalarOutputByPoolingWindowXYChartDataSource instantiateCopyOfDataSource()
    {
        return new ScalarOutputByPoolingWindowXYChartDataSource( getDataSourceOrderIndex(), getInput() );
    }

    @Override
    protected IntervalXYDataset instantiateXYDataset()
    {

        // Build the TimeSeriesCollection
        MetricOutputMapByTimeAndThreshold<ScalarOutput> input = getInput();
        TimeSeriesCollection returnMe = new TimeSeriesCollection();

        // Filter by lead time and then by threshold
        Set<Long> leadTimes = input.keySetByLeadTimeInHours();
        for ( Long nextTime : leadTimes )
        {
            MetricOutputMapByTimeAndThreshold<ScalarOutput> slice = input.filterByLeadTimeInHours( nextTime );
            // Filter by threshold
            Set<Threshold> thresholds = input.keySetByThreshold();
            for ( Threshold nextThreshold : thresholds )
            {
                MetricOutputMapByTimeAndThreshold<ScalarOutput> finalSlice = slice.filterByThreshold( nextThreshold );
                // Add the time-series
                TimeSeries next = new TimeSeries( nextTime + ", " + nextThreshold, FixedMillisecond.class );
                for ( Pair<TimeWindow, Threshold> key : finalSlice.keySet() )
                {
                    next.add( new FixedMillisecond( key.getLeft().getMidPointTime().toEpochMilli() ),
                              finalSlice.get( key ).getData() );
                }
                returnMe.addSeries( next );
            }
        }
        return returnMe;
    }

    @Override
    public int getXAxisType()
    {
        return ChartConstants.AXIS_IS_TIME;
    }
}
