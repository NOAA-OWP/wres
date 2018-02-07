package wres.vis;

import java.time.Duration;
import java.time.Instant;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.IntervalXYDataset;

import ohd.hseb.charter.ChartConstants;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.outputs.ScoreOutput;

/**
 * A chart data source for a {@link ScoreOutput} plotted by {@link TimeWindow#getMidPointTime()} on the 
 * domain axis, with individual series for each lead time and threshold.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class DurationOutputByBasisTimeXYChartDataSource
        extends WRESXYChartDataSource<MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>>>
{

    /**
     * @param orderIndex The data source order index within the plotted chart. This impacts some aspects of the display,
     *            such as the rendering order, legend order, and so forth.
     * @param input The data for which to display a chart.
     */
    public DurationOutputByBasisTimeXYChartDataSource( final int orderIndex,
                                                            final MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> input )
    {
        super( orderIndex, input, input.keySetByThreshold().size() * input.keySetByLeadTimeInHours().size() );

        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( "FORECAST ISSUE DATE/TIME [UTC]" );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( "@metricShortName@@metricComponentNameSuffix@@outputUnitsLabelSuffix@" );
        WRESTools.applyDefaultJFreeChartColorSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
        WRESTools.applyDefaultJFreeChartShapeSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
    }

    @Override
    protected DurationOutputByBasisTimeXYChartDataSource instantiateCopyOfDataSource()
    {
        return new DurationOutputByBasisTimeXYChartDataSource( getDataSourceOrderIndex(), getInput() );
    }

    @Override
    protected IntervalXYDataset instantiateXYDataset()
    {
        // Build the TimeSeriesCollection
        MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> input = getInput();
        TimeSeriesCollection returnMe = new TimeSeriesCollection();

        // Filter by lead time and then by threshold
        for ( Entry<Pair<TimeWindow, Threshold>, PairedOutput<Instant, Duration>> entry : input.entrySet() )
        {
            Long time = entry.getKey().getLeft().getLatestLeadTimeInHours();
            Threshold threshold = entry.getKey().getRight();
            TimeSeries next = new TimeSeries( time + ", " + threshold, FixedMillisecond.class );
            for ( Pair<Instant, Duration> oneValue : entry.getValue() )
            {
                next.add( new FixedMillisecond( oneValue.getLeft().toEpochMilli() ),
                          oneValue.getRight().toHours() );
            }
            returnMe.addSeries( next );
        }
        return returnMe;
    }

    @Override
    public int getXAxisType()
    {
        return ChartConstants.AXIS_IS_TIME;
    }
}
