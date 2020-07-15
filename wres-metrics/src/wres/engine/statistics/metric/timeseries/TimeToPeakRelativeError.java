package wres.engine.statistics.metric.timeseries;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Timestamp;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Metric;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;

/**
 * <p>Constructs a {@link Metric} that returns the fractional difference in time between the maximum values recorded in 
 * the left and right side of each time-series in the {@link PoolOfPairs}. The denominator in the fraction is 
 * given by the period, in hours, between the basis time and the time at which the maximum value is recorded in the 
 * left side of the paired input. Thus, for forecast time-series, the output is properly interpreted as the number of 
 * hours of error per hour of forecast lead time until the observed peak occurred.</p>
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
     * Canonical representation of the metric.
     */

    public static final DurationDiagramMetric METRIC = DurationDiagramMetric.newBuilder()
                                                                            .setName( MetricName.TIME_TO_PEAK_RELATIVE_ERROR )
                                                                            .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                     .setSeconds( Long.MIN_VALUE ) )
                                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                                     .setSeconds( Long.MIN_VALUE )
                                                                                                                     .setNanos( 999_999_999 ) )
                                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                                     .setSeconds( 0 ) )
                                                                            .build();

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeToPeakRelativeError.class );

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
    public DurationDiagramStatisticOuter apply( PoolOfPairs<Double, Double> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        // Iterate through the time-series by basis time, and find the peaks in left and right
        DurationDiagramStatistic.Builder builder = DurationDiagramStatistic.newBuilder()
                                                                           .setMetric( TimeToPeakError.METRIC );

        int sampleSize = 0;
        for ( TimeSeries<Pair<Double, Double>> next : s.get() )
        {
            // Some events?
            if ( !next.getEvents().isEmpty() )
            {
                // Get the first reference time
                Map<ReferenceTimeType, Instant> referenceTimes = next.getReferenceTimes();
                Map.Entry<ReferenceTimeType, Instant> firstEntry = referenceTimes.entrySet().iterator().next();
                Instant referenceTime = firstEntry.getValue();
                ReferenceTimeType referenceTimeType = firstEntry.getKey();

                if ( LOGGER.isTraceEnabled() )
                {
                    LOGGER.trace( "Using reference time {} with type {} for instance of {} with input hash {}.",
                                  referenceTime,
                                  referenceTimeType,
                                  TimeToPeakRelativeError.class,
                                  s.hashCode() );
                }

                Pair<Instant, Instant> peak = TimingErrorHelper.getTimeToPeak( next, this.getRNG() );

                // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
                Duration error = Duration.between( peak.getLeft(), peak.getRight() );

                // Compute the denominator
                Duration denominator = Duration.between( referenceTime, peak.getLeft() );

                // Add the relative time-to-peak error against the basis time
                // If the horizon is zero, the relative error is undefined
                // TODO: consider how to represent a NaN outcome within the framework of Duration, rather
                // than swallowing the outcome here

                if ( !denominator.isZero() )
                {
                    // Numerator seconds as a big decimal w/ nanos
                    BigDecimal errorSeconds = BigDecimal.valueOf( error.toSeconds() )
                                                        .add( BigDecimal.valueOf( error.get( ChronoUnit.NANOS ), 9 ) );
                    // Denominator seconds as a big decimal w/ nanos
                    BigDecimal denominatorSeconds =
                            BigDecimal.valueOf( denominator.toSeconds() )
                                      .add( BigDecimal.valueOf( denominator.get( ChronoUnit.NANOS ), 9 ) );

                    // Fraction
                    BigDecimal fraction = errorSeconds.divide( denominatorSeconds, RoundingMode.HALF_UP );

                    // Fractional seconds
                    BigDecimal seconds = fraction.multiply( BigDecimal.valueOf( 60.0 * 60.0 ) );

                    // Nearest whole second
                    seconds = seconds.setScale( 0, RoundingMode.HALF_UP );

                    PairOfInstantAndDuration pair = PairOfInstantAndDuration.newBuilder()
                                                                            .setTime( Timestamp.newBuilder()
                                                                                               .setSeconds( referenceTime.getEpochSecond() )
                                                                                               .setNanos( referenceTime.getNano() ) )
                                                                            .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                                      .setSeconds( seconds.longValue() ) )
                                                                            .build();

                    builder.addStatistics( pair );
                }

                sampleSize++;
            }
        }

        // Create output metadata with the identifier of the statistic as the component identifier
        StatisticMetadata meta = StatisticMetadata.of( s.getMetadata(),
                                                       sampleSize,
                                                       MeasurementUnit.of( "DURATION IN RELATIVE HOURS" ),
                                                       this.getID(),
                                                       MetricConstants.MAIN );

        return DurationDiagramStatisticOuter.of( builder.build(), meta );
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
