package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.VectorOutput;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class MeanSquareError<S extends SingleValuedPairs> extends SumOfSquareError<S>
{

    @Override
    public VectorOutput apply(final SingleValuedPairs s)
    {

        switch(getDecompositionID())
        {
            case NONE:
                return getMSENoDecomp(s);
            case CR:
            case LBR:
            case CR_AND_LBR:
            default:
                throw new UnsupportedOperationException("The Mean Square Error decomposition is not currently "
                    + "implemented.");
        }

    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class MeanSquareErrorBuilder<S extends SingleValuedPairs>
    extends
        DecomposableDoubleErrorScoreBuilder<S>
    {

        @Override
        protected MeanSquareError<S> build()
        {
            return new MeanSquareError<>(this);
        }

    }    
    
    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    MeanSquareError(final MeanSquareErrorBuilder<S> builder)
    {
        super(builder);
    }

    /**
     * Returns the Mean Square Error without any decomposition.
     * 
     * @param s the pairs
     * @return the mean square error without decomposition
     */

    private VectorOutput getMSENoDecomp(final SingleValuedPairs s)
    {
        double mse = getSumOfSquareError(s)/s.size();
        //Metadata
        final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.NONE, null);
        return getDataFactory().ofVectorOutput(new double[]{mse}, metOut);
    }

}
