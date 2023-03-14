package wres.metrics.timeseries;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Timestamp;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.metrics.Metric;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Constructs a {@link Metric} that returns the difference in time between the maximum values recorded in the left
 * and right side of each time-series in the {@link Pool}. For multiple peaks with the same value, the peak with the 
 * latest {@link Instant} is chosen. The timing error is measured with a {@link Duration}. A negative {@link Duration} 
 * indicates that the predicted peak was too early, i.e., occurred earlier than the observed peak.</p>
 * 
 * @author James Brown
 */
public class TimeToPeakError extends TimingError
{

    /**
     * Canonical representation of the metric.
     */

    public static final DurationDiagramMetric METRIC = DurationDiagramMetric.newBuilder()
                                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
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

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeToPeakError.class );

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
    public DurationDiagramStatisticOuter apply( Pool<TimeSeries<Pair<Double, Double>>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        // Iterate through the time-series by basis time, and find the peaks in left and right
        DurationDiagramStatistic.Builder builder = DurationDiagramStatistic.newBuilder()
                                                                           .setMetric( TimeToPeakError.METRIC );
        for ( TimeSeries<Pair<Double, Double>> next : s.get() )
        {
            // Some events?
            if ( !next.getEvents().isEmpty() )
            {
                Pair<Instant, Instant> peak = TimingErrorHelper.getTimeToPeak( next, this.getRNG() );

                // Duration.between is negative if the predicted/right or "end" is before the observed/left or "start"
                Duration error = Duration.between( peak.getLeft(), peak.getRight() );

                // Add the time-to-peak error against the first available reference time
                Map<ReferenceTimeType, Instant> referenceTimes = next.getReferenceTimes();
                Map.Entry<ReferenceTimeType, Instant> firstEntry = referenceTimes.entrySet().iterator().next();
                Instant referenceTime = firstEntry.getValue();
                ReferenceTimeType referenceTimeType = firstEntry.getKey();

                if ( LOGGER.isTraceEnabled() )
                {
                    LOGGER.trace( "Using reference time {} with type {} for instance of {} with input hash {}.",
                                  referenceTime,
                                  referenceTimeType,
                                  TimeToPeakError.class,
                                  s.hashCode() );
                }

                PairOfInstantAndDuration pair = PairOfInstantAndDuration.newBuilder()
                                                                        .setTime( Timestamp.newBuilder()
                                                                                           .setSeconds( referenceTime.getEpochSecond() )
                                                                                           .setNanos( referenceTime.getNano() ) )
                                                                        .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                                  .setSeconds( error.getSeconds() )
                                                                                                                  .setNanos( error.getNano() ) )
                                                                        .setReferenceTimeType( wres.statistics.generated.ReferenceTime.ReferenceTimeType.valueOf( referenceTimeType.name() ) )
                                                                        .build();

                builder.addStatistics( pair );
            }
        }

        return DurationDiagramStatisticOuter.of( builder.build(), s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
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
