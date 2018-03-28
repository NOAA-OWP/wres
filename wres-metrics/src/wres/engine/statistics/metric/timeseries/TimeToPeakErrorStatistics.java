package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.Collectable;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * A collection of summary statistics that operate on a {@link TimeToPeakError}.
 * 
 * TODO: consider implementing an API for summary statistics that works directly with {@link Duration}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeToPeakErrorStatistics extends TimeSummaryStatistics
        implements Collectable<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, DurationScoreOutput>
{

    /**
     * A {@link TimeToPeakError} to compute the intermediate output.
     */

    private final TimeToPeakError timeToPeakError;

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC;
    }

    @Override
    public PairedOutput<Instant, Duration> getCollectionInput( TimeSeriesOfSingleValuedPairs input )
    {
        return timeToPeakError.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.TIME_TO_PEAK_ERROR;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class TimeToPeakErrorStatisticBuilder extends TimeSummaryStatisticsBuilder
    {

        @Override
        public TimeToPeakErrorStatistics build() throws MetricParameterException
        {
            return new TimeToPeakErrorStatistics( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private TimeToPeakErrorStatistics( final TimeToPeakErrorStatisticBuilder builder ) throws MetricParameterException
    {
        super( builder );

        // Build the metric of which this is a collection
        timeToPeakError = MetricFactory.getInstance( getDataFactory() ).ofTimeToPeakError();
    }

}
