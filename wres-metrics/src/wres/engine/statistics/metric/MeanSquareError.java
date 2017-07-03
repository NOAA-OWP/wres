package wres.engine.statistics.metric;

import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MeanSquareError<S extends SingleValuedPairs> extends DecomposableDoubleErrorScore<S>
{

    /**
     * The decomposition identifier. See {@link MetricConstants#getDecompositionID()}.
     */

    private final MetricConstants decompositionID;

    @Override
    public VectorOutput apply(final SingleValuedPairs s)
    {

        switch(decompositionID)
        {
            case NONE:
                return getMSENoDecomp(s);
            case CR:
            case LBR:
            case CR_AND_LBR:
            default:
                throw new MetricCalculationException("The Mean Square Error decomposition is not currently "
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
    public boolean isDecomposable()
    {
        return true;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public MetricConstants getDecompositionID()
    {
        return decompositionID;
    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    protected MeanSquareError(final MeanSquareErrorBuilder<S> b)
    {
        super(b.outputFactory);
        if(!Score.isSupportedDecompositionID(b.decompositionID))
        {
            throw new IllegalStateException("Unrecognized decomposition identifier: " + b.decompositionID);
        }
        this.decompositionID = b.decompositionID;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class MeanSquareErrorBuilder<S extends SingleValuedPairs> extends MetricBuilder<S, VectorOutput>
    {
        /**
         * The type of metric decomposition. See {@link MetricConstants#getDecompositionID()}.
         */

        private MetricConstants decompositionID = MetricConstants.NONE;

        @Override
        protected MeanSquareError<S> build()
        {
            return new MeanSquareError<>(this);
        }

        protected MeanSquareErrorBuilder<S> setDecompositionID(final MetricConstants decompositionID)
        {
            this.decompositionID = decompositionID;
            return this;
        }
    }

    /**
     * Returns the Mean Square Error without any decomposition.
     * 
     * @param s the pairs
     * @return the mean square error without decomposition.
     */

    private VectorOutput getMSENoDecomp(final SingleValuedPairs s)
    {
        final double[] result = new double[]{
            s.getData().stream().mapToDouble(FunctionFactory.squareError()).average().getAsDouble()};
        //Metadata
        final Metadata metIn = s.getMetadata();
        final MetricOutputMetadata metOut = MetadataFactory.getMetadata(metIn.getSampleSize(),
                                                                        metIn.getDimension(),
                                                                        getID(),
                                                                        MetricConstants.MAIN,
                                                                        metIn.getID(),
                                                                        null);
        return getOutputFactory().ofVectorOutput(result, metOut);
    }

}
