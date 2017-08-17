package wres.engine.statistics.metric;

import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;

/**
 * As with the MSE, the Root Mean Square Error (RMSE) or Root Mean Square Deviation (RMSD) is a measure of accuracy.
 * However, the RMSE is expressed in the original (unsquared) units of the predictand and no decompositions are
 * available for the RMSE.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class RootMeanSquareError extends DoubleErrorScore<SingleValuedPairs>
{

    @Override
    public ScalarOutput apply(final SingleValuedPairs t)
    {      
        final ScalarOutput intermediate = super.apply(t);
        return getDataFactory().ofScalarOutput(Math.pow(intermediate.getData(), 0.5),intermediate.getMetadata());
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.ROOT_MEAN_SQUARE_ERROR;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public MetricDecompositionGroup getDecompositionID()
    {
        return MetricDecompositionGroup.NONE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class RootMeanSquareErrorBuilder extends DoubleErrorScoreBuilder<SingleValuedPairs>
    {

        @Override
        protected RootMeanSquareError build()
        {
            return new RootMeanSquareError(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private RootMeanSquareError(final RootMeanSquareErrorBuilder builder)
    {
        super(builder.setErrorFunction(FunctionFactory.squareError()));
    }

}
