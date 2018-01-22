package wres.engine.statistics.metric.singlevalued;

import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * Abstract base class for decomposable scores that involve a sum-of-square errors.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
abstract class SumOfSquareError<S extends SingleValuedPairs> extends DecomposableScore<S>
{

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    SumOfSquareError( final DecomposableScoreBuilder<S> builder ) throws MetricParameterException
    {
        super( builder );
    }

    /**
     * Returns the sum of square errors for the input.
     * 
     * @param s the input
     * @return the sum of square errors
     */

    protected double getSumOfSquareError( final SingleValuedPairs s )
    {
        return s.getData().stream().mapToDouble( FunctionFactory.squareError() ).sum();
    }

}
