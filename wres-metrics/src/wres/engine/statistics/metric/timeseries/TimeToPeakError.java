package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Metric;

/**
 * <p>Constructs a {@link Metric} that returns the difference in time between the maximum values recorded in the left
 * and right side of each time-series in the {@link TimeSeriesOfSingleValuedPairs}. For multiple peaks with the same
 * value, the peak with the latest {@link Instant} is chosen. The timing error is measured with a {@link Duration}. A
 * negative {@link Duration} indicates that the predicted peak is after the observed peak.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeToPeakError extends TimingError
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static TimeToPeakError of()
    {
        return new TimeToPeakError();
    }

    /**
     * Returns an instance with a prescribed random number generator for resolving ties.
     * 
     * @param rng the random number generator for resolving ties
     * @return an instance
     */

    public static TimeToPeakError of( Random rng )
    {
        return new TimeToPeakError( rng );
    }

    @Override
    public PairedStatistic<Instant, Duration> apply( TimeSeriesOfSingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        // Iterate through the time-series by basis time, and find the peaks in left and right
        List<Pair<Instant, Duration>> returnMe = new ArrayList<>();
        for ( TimeSeries<SingleValuedPair> next : s.getTimeSeries() )
        {
            Pair<Instant, Instant> peak = TimingErrorHelper.getTimeToPeak( next, this.getRNG() );

            // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
            Duration error = Duration.between( peak.getLeft(), peak.getRight() );

            // Add the time-to-peak error against the basis time
            returnMe.add( Pair.of( next.getReferenceTime(), error ) );
        }

        // Create output metadata
        StatisticMetadata meta = StatisticMetadata.of( s.getMetadata(),
                                                       s.getTimeSeries().size(),
                                                       MeasurementUnit.of( "DURATION" ),
                                                       this.getID(),
                                                       MetricConstants.MAIN );

        return PairedStatistic.of( returnMe, meta );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.TIME_TO_PEAK_ERROR;
    }

    /**
     * Hidden constructor.
     */

    private TimeToPeakError()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param rng the random number generator for resolving ties 
     */

    private TimeToPeakError( Random rng )
    {
        super( rng );
    }

}
