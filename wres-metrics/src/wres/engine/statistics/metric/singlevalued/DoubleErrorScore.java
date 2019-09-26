package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.DoubleErrorFunction;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.OrdinaryScore;

/**
 * A generic implementation of an error score that cannot be decomposed. For scores that can be computed in a single-pass,
 * provide a {@link DoubleErrorFunction} to the constructor. This function is applied to each pair, and the average
 * score returned across all pairs.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class DoubleErrorScore<S extends SampleData<Pair<Double, Double>>>
        extends OrdinaryScore<S, DoubleScoreStatistic>
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
     * Partial message on null input.
     */

    private static final String NULL_INPUT_STRING = "Cannot construct the error score '";

    @Override
    public DoubleScoreStatistic apply( final S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        //Metadata
        DatasetIdentifier id = null;
        if ( s.hasBaseline() )
        {
            id = s.getBaselineData().getMetadata().getIdentifier();
        }
        final StatisticMetadata metOut =
                StatisticMetadata.of( s.getMetadata(),
                                      this.getID(),
                                      MetricConstants.MAIN,
                                      this.hasRealUnits(),
                                      s.getRawData().size(),
                                      id );

        //Compute the atomic errors in a stream
        double doubleScore = MissingValues.MISSING_DOUBLE;
        if ( !s.getRawData().isEmpty() )
        {
            double[] doubles = s.getRawData().stream().mapToDouble( this.getErrorFunction() ).toArray();
            VectorOfDoubles wrappedDoubles = VectorOfDoubles.of( doubles );
            doubleScore = this.getErrorAccumulator().applyAsDouble( wrappedDoubles );
        }
        return DoubleScoreStatistic.of( doubleScore, metOut );
    }

    @Override
    public ScoreGroup getScoreOutputGroup()
    {
        return ScoreGroup.NONE;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    /**
     * Construct an error score with a default error function {@link FunctionFactory#error()}
     * and a default accumulator {@link FunctionFactory#mean()}.
     */

    DoubleErrorScore()
    {
        super();

        this.errorFunction = FunctionFactory.error();
        this.errorAccumulator = FunctionFactory.mean();
    }

    /**
     * Construct an error score with a default accumulator {@link FunctionFactory#mean()}.
     * 
     * @param function the error function
     * @throws NullPointerException if the error function is null
     */

    DoubleErrorScore( DoubleErrorFunction function )
    {
        super();

        Objects.requireNonNull( function,
                                NULL_INPUT_STRING + this.getName()
                                          + "' with a null error function." );

        this.errorFunction = function;
        this.errorAccumulator = FunctionFactory.mean();
    }

    /**
     * Construct an error score.
     * 
     * @param function the error function
     * @param errorAccumulator the error accumulator function 
     * @throws NullPointerException if either input is null
     */

    DoubleErrorScore( DoubleErrorFunction function, ToDoubleFunction<VectorOfDoubles> errorAccumulator )
    {
        super();

        Objects.requireNonNull( function,
                                NULL_INPUT_STRING + this.getName()
                                          + "' with a null error function." );

        Objects.requireNonNull( errorAccumulator,
                                NULL_INPUT_STRING + this.getName()
                                                  + "' with a null accumulator function." );

        this.errorFunction = function;
        this.errorAccumulator = errorAccumulator;
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
