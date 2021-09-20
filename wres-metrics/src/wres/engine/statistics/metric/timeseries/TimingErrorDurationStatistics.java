package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * A collection of timing error summary statistics that consume a {@link Pool} of doubles and produce a 
 * {@link DurationScoreStatisticOuter}.
 * 
 * TODO: consider implementing an API for summary statistics that works directly with {@link Duration}.
 * 
 * @author James Brown
 */
public class TimingErrorDurationStatistics
        implements Metric<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter>
{

    /**
     * The summary statistics and associated identifiers
     */

    private final Map<MetricConstants, ToDoubleFunction<VectorOfDoubles>> statistics;

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
     * @param statistics the list of statistics the compute, not null
     * @throws MetricParameterException if one or more parameters is invalid
     * @return an instance
     */

    public static TimingErrorDurationStatistics of( TimingError timingError,
                                                    Set<MetricConstants> statistics )
            throws MetricParameterException
    {
        return new TimingErrorDurationStatistics( timingError, statistics );
    }

    @Override
    public DurationScoreStatisticOuter apply( Pool<TimeSeries<Pair<Double, Double>>> pairs )
    {
        if ( Objects.isNull(pairs ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        DurationDiagramStatisticOuter statistics = this.timingError.apply( pairs );

        // Map of outputs
        DurationScoreMetric.Builder metricBuilder =
                DurationScoreMetric.newBuilder().setName( MetricName.valueOf( this.getMetricName().name() ) );
        DurationScoreStatistic.Builder scoreBuilder = DurationScoreStatistic.newBuilder();

        // Iterate through the statistics
        MetricConstants nextIdentifier = null;
        for ( Entry<MetricConstants, ToDoubleFunction<VectorOfDoubles>> next : this.statistics.entrySet() )
        {
            nextIdentifier = next.getKey();

            // Data available
            if ( statistics.getData().getStatisticsCount() != 0 )
            {
                // Convert the input to double ms
                double[] input = statistics.getData()
                                           .getStatisticsList()
                                           .stream()
                                           .mapToDouble( a -> ( a.getDuration()
                                                                 .getSeconds()
                                                                * 1000 )
                                                              + ( a.getDuration()
                                                                   .getNanos()
                                                                  / 1_000_000 ) )
                                           .toArray();

                // Some loss of precision here, not consequential
                Duration duration = Duration.ofMillis( Math.round( this.statistics.get( nextIdentifier )
                                                                                  .applyAsDouble( VectorOfDoubles.of( input ) ) ) );

                // Add statistic component
                DurationScoreMetricComponent componentMetric = this.components.get( nextIdentifier );
                DurationScoreStatisticComponent.Builder builder = DurationScoreStatisticComponent.newBuilder()
                                                                                                 .setMetric( componentMetric );

                builder.setValue( MessageFactory.parse( duration ) );

                scoreBuilder.addStatistics( builder );
            }
        }

        DurationScoreStatistic score = scoreBuilder.setMetric( metricBuilder ).build();

        return DurationScoreStatisticOuter.of( score, statistics.getMetadata() );
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
     * @param statistics the list of statistics the compute, not null
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

        String attemptedName = timingError.getMetricName().name() + "_STATISTIC";
        try
        {
            this.identifier = MetricConstants.valueOf( attemptedName );
        }
        catch ( IllegalArgumentException e )
        {
            throw new MetricParameterException( "Unexpected timing error metric: no summary statisitcs are available "
                                                + "with the name "
                                                + attemptedName
                                                + "." );
        }

        // Copy locally
        Set<MetricConstants> input = new HashSet<>( statistics );

        // Validate
        if ( input.isEmpty() )
        {
            throw new MetricParameterException( "Specify one or more summary statistics." );
        }

        this.statistics = new TreeMap<>();
        this.components = new EnumMap<>( MetricConstants.class );

        // Set and validate the copy
        for ( MetricConstants next : input )
        {
            if ( Objects.isNull( next ) )
            {
                throw new MetricParameterException( "Cannot build the metric with a null statistic." );
            }
            this.statistics.put( next, FunctionFactory.ofStatistic( next ) );

            DurationScoreMetricComponent component = this.getMetricDescription( next );
            this.components.put( next, component );
        }

        this.timingError = timingError;
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
        ComponentName componentName = ComponentName.valueOf( identifier.name() );
        DurationScoreMetricComponent.Builder builder = DurationScoreMetricComponent.newBuilder()
                                                                                   .setName( componentName );

        switch ( identifier )
        {
            case MEAN:
            case MEDIAN:
            case MINIMUM:
            case MAXIMUM:
                builder.setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                .setSeconds( Long.MIN_VALUE ) )
                       .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                .setSeconds( Long.MAX_VALUE )
                                                                .setNanos( 999_999_999 ) )
                       .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                .setSeconds( 0 ) );
                break;
            case MEAN_ABSOLUTE:
            case STANDARD_DEVIATION:
                builder.setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                .setSeconds( 0 ) )
                       .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                .setSeconds( Long.MAX_VALUE )
                                                                .setNanos( 999_999_999 ) )
                       .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                .setSeconds( 0 ) );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized duration score metric '" + identifier + "'." );
        }

        return builder.build();
    }

}
