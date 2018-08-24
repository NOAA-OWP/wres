package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.PairedOutput;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Metric;

/**
 * <p>Constructs a {@link Metric} that returns the fractional difference in time between the maximum values recorded in 
 * the left and right side of each time-series in the {@link TimeSeriesOfSingleValuedPairs}. The denominator in the 
 * fraction is given by the period, in hours, between the basis time and the time at which the maximum value is 
 * recorded in the left side of the paired input. Thus, for forecast time-series, the output is properly interpreted 
 * as the number of hours of error per hour of forecast lead time until the observed peak occurred.</p>
 * 
 * <p>For multiple peaks with the same value, the peak with the latest {@link Instant} is chosen. The timing error is 
 * measured with a {@link Duration}. However, the fraction is measured in relative hours, i.e. the timing error 
 * is divided by a <code>long</code> value of hours using {@link Duration#dividedBy(long)}. A negative {@link Duration} 
 * indicates that the predicted peak is after the observed peak.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimeToPeakRelativeError extends TimingError
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static TimeToPeakRelativeError of()
    {
        return new TimeToPeakRelativeError();
    }

    /**
     * Returns an instance with a prescribed random number generator for resolving ties.
     * 
     * @param rng the random number generator for resolving ties
     * @return an instance
     */

    public static TimeToPeakRelativeError of( Random rng )
    {
        return new TimeToPeakRelativeError( rng );
    }

    @Override
    public PairedOutput<Instant, Duration> apply( TimeSeriesOfSingleValuedPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        // Iterate through the time-series by basis time, and find the peaks in left and right
        List<Pair<Instant, Duration>> returnMe = new ArrayList<>();
        for ( TimeSeries<SingleValuedPair> next : s.basisTimeIterator() )
        {
            Pair<Instant, Instant> peak = TimingErrorHelper.getTimeToPeak( next, this.getRNG() );

            // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
            Duration error = Duration.between( peak.getLeft(), peak.getRight() );

            // Compute the denominator
            Duration denominator = Duration.between( next.getEarliestBasisTime(), peak.getLeft() );
            long denominatorHours = denominator.toHours();

            // Add the relative time-to-peak error against the basis time
            // If the horizon is zero, the relative error is undefined
            // TODO: consider how to represent a NaN outcome within the framework of Duration, rather
            // than swallowing the outcome here
            if ( denominatorHours != 0 )
            {
                returnMe.add( Pair.of( next.getEarliestBasisTime(), error.dividedBy( denominatorHours ) ) );
            }
        }

        // Create output metadata with the identifier of the statistic as the component identifier
        MetricOutputMetadata meta = MetricOutputMetadata.of( s.getMetadata(),
                                                             s.getBasisTimes().size(),
                                                             MeasurementUnit.of( "DURATION IN RELATIVE HOURS" ),
                                                             this.getID(),
                                                             MetricConstants.MAIN );

        return PairedOutput.of( returnMe, meta );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    private TimeToPeakRelativeError()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param rng the random number generator for resolving ties
     */

    private TimeToPeakRelativeError( Random rng )
    {
        super( rng );
    }

}
