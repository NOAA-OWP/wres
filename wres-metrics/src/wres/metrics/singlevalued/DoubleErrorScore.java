package wres.metrics.singlevalued;

import java.util.Objects;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.DoubleErrorFunction;
import wres.metrics.FunctionFactory;
import wres.metrics.Score;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * A generic implementation of an error score that cannot be decomposed. For scores that can be computed in a single-pass,
 * provide a {@link DoubleErrorFunction} to the constructor. This function is applied to each pair, and the average
 * score returned across all pairs.
 * 
 * @param <S> the type of pooled data
 * @author James Brown
 */
public abstract class DoubleErrorScore<S extends Pool<Pair<Double, Double>>>
        implements Score<S, DoubleScoreStatisticOuter>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DoubleErrorScore.class );

    /** The error function. */
    final DoubleErrorFunction errorFunction;

    /** The error accumulator function. */
    final ToDoubleFunction<VectorOfDoubles> errorAccumulator;

    /** The metric description.*/
    final DoubleScoreMetric metric;

    /** Partial message on null input. */
    private static final String NULL_INPUT_STRING = "Cannot construct the error score '";

    @Override
    public DoubleScoreStatisticOuter apply( final S pool )
    {
        LOGGER.debug( "Computing the {}.", this );

        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        //Compute the atomic errors in a stream
        double doubleScore = MissingValues.DOUBLE;
        if ( !pool.get().isEmpty() )
        {
            double[] doubles = pool.get()
                                   .stream()
                                   .mapToDouble( this.getErrorFunction() )
                                   .toArray();
            VectorOfDoubles wrappedDoubles = VectorOfDoubles.of( doubles );
            doubleScore = this.getErrorAccumulator()
                              .applyAsDouble( wrappedDoubles );
        }

        Optional<DoubleScoreMetricComponent> main = this.metric.getComponentsList()
                                                               .stream()
                                                               .filter( next -> next.getName() == ComponentName.MAIN )
                                                               .findFirst();

        DoubleScoreStatisticComponent.Builder component = DoubleScoreStatisticComponent.newBuilder()
                                                                                       .setValue( doubleScore );

        if ( main.isPresent() )
        {
            DoubleScoreMetricComponent toSet = main.get();

            // If the metric units are not explicitly set, they are paired units
            if ( toSet.getUnits()
                      .isBlank() )
            {
                String unit = pool.getMetadata()
                                  .getMeasurementUnit()
                                  .toString();
                toSet = toSet.toBuilder()
                             .setUnits( unit )
                             .build();
                if ( LOGGER.isTraceEnabled() )
                {
                    LOGGER.trace( "Setting the measurement units for score metric component {} to {}.",
                                  toSet.getName().name(),
                                  unit );
                }
            }

            component.setMetric( toSet );
        }

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    // Basic metric description at the top level, component description at the 
                                    // component level
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( this.metric.getName() ) )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return this.getMetricName()
                   .toString();
    }

    /**
     * Construct an error score with a default error function {@link FunctionFactory#error()}
     * and a default accumulator {@link FunctionFactory#mean()}.
     * 
     * @param metric the metric description
     * @throws NullPointerException if the input is null
     */

    DoubleErrorScore( DoubleScoreMetric metric )
    {
        super();

        Objects.requireNonNull( metric );

        this.errorFunction = FunctionFactory.error();
        this.errorAccumulator = FunctionFactory.mean();
        this.metric = metric;
    }

    /**
     * Construct an error score with a default accumulator {@link FunctionFactory#mean()}.
     * 
     * @param function the error function
     * @param metric the metric description 
     * @throws NullPointerException if either input is null
     */

    DoubleErrorScore( DoubleErrorFunction function, DoubleScoreMetric metric )
    {
        super();

        Objects.requireNonNull( metric );
        Objects.requireNonNull( function,
                                NULL_INPUT_STRING + this.getName()
                                          + "' with a null error function." );

        this.errorFunction = function;
        this.errorAccumulator = FunctionFactory.mean();
        this.metric = metric;
    }

    /**
     * Construct an error score.
     * 
     * @param function the error function
     * @param errorAccumulator the error accumulator function 
     * @param metric the metric description
     * @throws NullPointerException if any input is null
     */

    DoubleErrorScore( DoubleErrorFunction function,
                      ToDoubleFunction<VectorOfDoubles> errorAccumulator,
                      DoubleScoreMetric metric )
    {
        super();

        Objects.requireNonNull( metric );
        Objects.requireNonNull( function,
                                NULL_INPUT_STRING + this.getName()
                                          + "' with a null error function." );

        Objects.requireNonNull( errorAccumulator,
                                NULL_INPUT_STRING + this.getName()
                                                  + "' with a null accumulator function." );

        this.errorFunction = function;
        this.errorAccumulator = errorAccumulator;
        this.metric = metric;
    }

    /**
     * Returns the error function, not to be exposed.
     * 
     * @return the error function for internal use
     */

    private DoubleErrorFunction getErrorFunction()
    {
        return this.errorFunction;
    }

    /**
     * Returns the error accumulator, not to be exposed.
     * 
     * @return the error accumulator for internal use
     */

    private ToDoubleFunction<VectorOfDoubles> getErrorAccumulator()
    {
        return this.errorAccumulator;
    }
}
