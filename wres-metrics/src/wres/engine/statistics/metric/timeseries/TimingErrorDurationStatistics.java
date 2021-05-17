package wres.engine.statistics.metric.timeseries;

import java.time.Duration;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * A collection of summary statistics that operate on the outputs from {@link TimingError} and are expressed as 
 * {@link DurationScoreStatisticOuter}.
 * 
 * TODO: consider implementing an API for summary statistics that works directly with {@link Duration}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class TimingErrorDurationStatistics
        implements Function<DurationDiagramStatisticOuter, DurationScoreStatisticOuter>
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
     * The metric name.
     */

    private final MetricConstants identifier;

    /**
     * Returns an instance.
     * 
     * @param identifier the unique identifier for the summary statistics
     * @param statistics the list of statistics the compute
     * @throws MetricParameterException if one or more parameters is invalid
     * @return an instance
     */

    public static TimingErrorDurationStatistics of( MetricConstants identifier, Set<MetricConstants> statistics )
            throws MetricParameterException
    {
        return new TimingErrorDurationStatistics( identifier, statistics );
    }


    @Override
    public DurationScoreStatisticOuter apply( DurationDiagramStatisticOuter pairs )
    {
        if ( Objects.isNull( pairs ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        // Map of outputs
        DurationScoreMetric.Builder metricBuilder =
                DurationScoreMetric.newBuilder().setName( MetricName.valueOf( this.identifier.name() ) );
        DurationScoreStatistic.Builder scoreBuilder = DurationScoreStatistic.newBuilder();

        // Iterate through the statistics
        MetricConstants nextIdentifier = null;
        for ( Entry<MetricConstants, ToDoubleFunction<VectorOfDoubles>> next : this.statistics.entrySet() )
        {
            nextIdentifier = next.getKey();

            // Data available
            if ( pairs.getData().getStatisticsCount() != 0 )
            {
                // Convert the input to double ms
                double[] input = pairs.getData()
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

        return DurationScoreStatisticOuter.of( score, pairs.getMetadata() );
    }

    /**
     * Hidden constructor.
     * 
     * @param identifier the unique identifier for the summary statistics
     * @param statistics the list of statistics the compute
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private TimingErrorDurationStatistics( MetricConstants identifier, Set<MetricConstants> statistics )
            throws MetricParameterException
    {

        if ( Objects.isNull( identifier ) )
        {
            throw new MetricParameterException( "Specify a unique identifier from which to build the statistics." );
        }

        if ( Objects.isNull( statistics ) )
        {
            throw new MetricParameterException( "Specify a non-null container of summary statistics." );
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

        this.identifier = identifier;
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
