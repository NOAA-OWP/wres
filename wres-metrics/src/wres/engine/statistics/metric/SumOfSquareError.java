package wres.engine.statistics.metric;

import wres.datamodel.metric.SingleValuedPairs;

/**
 * Abstract base class for decomposable scores that involve a sum-of-square errors.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class SumOfSquareError<S extends SingleValuedPairs> extends DecomposableDoubleErrorScore<S>
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
     */

    protected SumOfSquareError(final DecomposableDoubleErrorScoreBuilder<S> builder)
    {
        super(builder);
    }

    /**
     * Returns the sum of square errors for the input.
     * 
     * @param input the input
     * @return the sum of square errors
     */

    double getSumOfSquareError(final SingleValuedPairs s)
    {
        return s.getData().stream().mapToDouble(FunctionFactory.squareError()).sum();
    }

}
