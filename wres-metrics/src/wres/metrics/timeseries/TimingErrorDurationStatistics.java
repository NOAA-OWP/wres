package wres.metrics.timeseries;

import java.time.Duration;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.metrics.FunctionFactory;
import wres.metrics.Metric;
import wres.metrics.MetricParameterException;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * <p>A collection of timing error summary statistics that consume a {@link Pool} of doubles and produce a
 * {@link DurationScoreStatisticOuter}.
 *
 * @author James Brown
 */
public class TimingErrorDurationStatistics
        implements Metric<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter>
{
    /**
     * The summary statistics and associated identifiers
     */

    private final Map<MetricConstants, Function<Duration[], Duration>> statistics;

    /**
     * A map of score metric components by name.
     */

    private final Map<MetricConstants, DurationScoreMetricComponent> components;

    /**
     * The underlying measure of timing error.
     */

    private final TimingError timingError;

    /**
     * The name of this metric.
     */

    private final MetricConstants identifier;

    /**
     * Returns an instance.
     *
     * @param timingError the underlying measure of timing error, not null
     * @param statistics the list of statistics to compute, not null
     * @throws MetricParameterException if one or more parameters is invalid
     * @return an instance
     */

    public static TimingErrorDurationStatistics of( TimingError timingError,
                                                    Set<MetricConstants> statistics )
    {
        return new TimingErrorDurationStatistics( timingError, statistics );
    }

    @Override
    public DurationScoreStatisticOuter apply( Pool<TimeSeries<Pair<Double, Double>>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        DurationDiagramStatisticOuter statisticsInner = this.timingError.apply( pool );

        // Map of outputs
        DurationScoreMetric.Builder metricBuilder =
                DurationScoreMetric.newBuilder()
                                   .setName( MetricName.valueOf( this.getMetricName()
                                                                     .name() ) );
        DurationScoreStatistic.Builder scoreBuilder = DurationScoreStatistic.newBuilder();

        // Iterate through the statistics
        for ( Entry<MetricConstants, Function<Duration[], Duration>> next : this.statistics.entrySet() )
        {
            MetricConstants nextIdentifier = next.getKey();
            Function<Duration[], Duration> nextFunction = next.getValue();

            // Data available
            if ( statisticsInner.getStatistic()
                                .getStatisticsCount() != 0 )
            {
                // Convert the java durations
                Duration[] input = statisticsInner.getStatistic()
                                                  .getStatisticsList()
                                                  .stream()
                                                  .map( DurationDiagramStatistic.PairOfInstantAndDuration::getDuration )
                                                  .map( MessageUtilities::getDuration )
                                                  .toArray( Duration[]::new);

                Duration duration = nextFunction.apply( input );

                // Add statistic component
                DurationScoreMetricComponent componentMetric = this.components.get( nextIdentifier );
                DurationScoreStatisticComponent.Builder builder
                        = DurationScoreStatisticComponent.newBuilder()
                                                         .setMetric( componentMetric );

                builder.setValue( MessageUtilities.getDuration( duration ) );

                scoreBuilder.addStatistics( builder );
            }
        }

        DurationScoreStatistic score = scoreBuilder.setMetric( metricBuilder )
                                                   .build();

        return DurationScoreStatisticOuter.of( score, statisticsInner.getPoolMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return this.identifier;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     *
     * @param timingError the underlying measure of timing error, not null
     * @param statistics the list of statistics to compute, not null
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private TimingErrorDurationStatistics( TimingError timingError,
                                           Set<MetricConstants> statistics )
    {
        if ( Objects.isNull( timingError ) )
        {
            throw new MetricParameterException( "Specify a timing error metric from which to build the statistics." );
        }

        if ( Objects.isNull( statistics ) )
        {
            throw new MetricParameterException( "Specify a non-null container of summary statistics." );
        }

        // Validate
        if ( statistics.isEmpty() )
        {
            throw new MetricParameterException( "Specify one or more summary statistics." );
        }

        Map<MetricConstants, Function<Duration[], Duration>> innerStatistics = new TreeMap<>();
        Map<MetricConstants, DurationScoreMetricComponent> innerComponents = new EnumMap<>( MetricConstants.class );

        // Set and validate the copy
        for ( MetricConstants next : statistics )
        {
            if ( Objects.isNull( next ) )
            {
                throw new MetricParameterException( "Cannot build the metric with a null statistic." );
            }

            MetricConstants nextSummaryStatistic = next.getChild();
            // Try the parent name if no child
            if ( Objects.isNull( nextSummaryStatistic ) )
            {
                nextSummaryStatistic = next;
            }

            ToDoubleFunction<double[]> univariate = FunctionFactory.ofScalarSummaryStatistic( nextSummaryStatistic );
            Function<Duration[], Duration> duration = FunctionFactory.ofDurationFromUnivariateFunction( univariate );
            innerStatistics.put( next, duration );

            DurationScoreMetricComponent component = this.getMetricDescription( next );
            innerComponents.put( next, component );
        }

        this.statistics = Collections.unmodifiableMap( innerStatistics );
        this.components = Collections.unmodifiableMap( innerComponents );
        this.timingError = timingError;

        // Identify the collection
        MetricConstants statistic = statistics.iterator()
                                              .next();
        this.identifier = statistic.getCollection();
    }

    /**
     * Returns a metric description for the identifier.
     *
     * @param identifier the metric identifier
     * @return a metric description
     * @throws IllegalArgumentException if the identifier is not recognized
     */

    private DurationScoreMetricComponent getMetricDescription( MetricConstants identifier )
    {
        String summaryNameString = identifier.getChild()
                                             .name();
        MetricName componentName = MetricName.valueOf( summaryNameString );
        DurationScoreMetricComponent.Builder builder = DurationScoreMetricComponent.newBuilder()
                                                                                   .setName( componentName );

        switch ( componentName )
        {
            case MEAN, MEDIAN, MINIMUM, MAXIMUM -> builder.setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                   .setSeconds( Long.MIN_VALUE ) )
                                                          .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                   .setSeconds( Long.MAX_VALUE )
                                                                                                   .setNanos(
                                                                                                           999_999_999 ) )
                                                          .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                                   .setSeconds( 0 ) );
            case MEAN_ABSOLUTE, STANDARD_DEVIATION -> builder.setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                      .setSeconds( 0 ) )
                                                             .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                      .setSeconds( Long.MAX_VALUE )
                                                                                                      .setNanos(
                                                                                                              999_999_999 ) )
                                                             .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                                      .setSeconds( 0 ) );
            default -> throw new IllegalArgumentException( "Unrecognized duration score metric '" + identifier + "'." );
        }

        return builder.build();
    }

}