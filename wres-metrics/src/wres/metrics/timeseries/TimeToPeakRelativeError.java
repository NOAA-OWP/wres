package wres.metrics.timeseries;

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

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.metrics.Metric;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;

/**
 * <p>Constructs a {@link Metric} that returns the fractional difference in time between the maximum values recorded in 
 * the left and right side of each time-series in the {@link Pool}. The denominator in the fraction is given by the 
 * period, in hours, between the basis time and the time at which the maximum value is recorded in the left side of the 
 * paired input. Thus, for forecast time-series, the output is properly interpreted as the duration per hour of forecast 
 * lead time until the observed peak occurred.</p>
 * 
 * <p>For multiple peaks with the same value, the peak with the latest {@link Instant} is chosen. A negative 
 * {@link Duration} indicates that the predicted peak was too early, i.e., occurred earlier than the observed peak.</p>
 * 
 * @author James Brown
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
                                                                                                                     .setSeconds( Long.MAX_VALUE )
                                                                                                                     .setNanos( 999_999_999 ) )
                                                                            .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                     .setSeconds( 0 ) )
                                                                            .build();

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeToPeakRelativeError.class );

    /**
     * Number of seconds in an hour.
     */

    private static final BigDecimal SECONDS_PER_HOUR = BigDecimal.valueOf( 60.0 * 60.0 );

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
    public DurationDiagramStatisticOuter apply( Pool<TimeSeries<Pair<Double, Double>>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        // Iterate through the time-series by basis time, and find the peaks in left and right
        DurationDiagramStatistic.Builder builder = DurationDiagramStatistic.newBuilder()
                                                                           .setMetric( TimeToPeakRelativeError.METRIC );

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
                    BigDecimal seconds = fraction.multiply( TimeToPeakRelativeError.SECONDS_PER_HOUR );

                    // Nearest whole second
                    seconds = seconds.setScale( 0, RoundingMode.HALF_UP );

                    PairOfInstantAndDuration pair = PairOfInstantAndDuration.newBuilder()
                                                                            .setTime( Timestamp.newBuilder()
                                                                                               .setSeconds( referenceTime.getEpochSecond() )
                                                                                               .setNanos( referenceTime.getNano() ) )
                                                                            .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                                      .setSeconds( seconds.longValue() ) )
                                                                            .setReferenceTimeType( wres.statistics.generated.ReferenceTime.ReferenceTimeType.valueOf( referenceTimeType.name() ) )
                                                                            .build();

                    builder.addStatistics( pair );
                }
            }
        }

        return DurationDiagramStatisticOuter.of( builder.build(), s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
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
