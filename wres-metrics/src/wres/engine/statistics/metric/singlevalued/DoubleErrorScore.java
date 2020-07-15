package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.MissingValues;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.DoubleErrorFunction;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.OrdinaryScore;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * A generic implementation of an error score that cannot be decomposed. For scores that can be computed in a single-pass,
 * provide a {@link DoubleErrorFunction} to the constructor. This function is applied to each pair, and the average
 * score returned across all pairs.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class DoubleErrorScore<S extends SampleData<Pair<Double, Double>>>
        extends OrdinaryScore<S, DoubleScoreStatisticOuter>
{
    /**
     * The error function.
     */

    final DoubleErrorFunction errorFunction;

    /**
     * The error accumulator function.
     */

    final ToDoubleFunction<VectorOfDoubles> errorAccumulator;

    /**
     * The metric description.
     */

    final DoubleScoreMetric metric;

    /**
     * Partial message on null input.
     */

    private static final String NULL_INPUT_STRING = "Cannot construct the error score '";

    @Override
    public DoubleScoreStatisticOuter apply( final S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        //Compute the atomic errors in a stream
        double doubleScore = MissingValues.DOUBLE;
        if ( !s.getRawData().isEmpty() )
        {
            double[] doubles = s.getRawData().stream().mapToDouble( this.getErrorFunction() ).toArray();
            VectorOfDoubles wrappedDoubles = VectorOfDoubles.of( doubles );
            doubleScore = this.getErrorAccumulator().applyAsDouble( wrappedDoubles );
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( doubleScore )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( this.metric )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, s.getMetadata() );
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
