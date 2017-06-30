package wres.engine.statistics.metric;

import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * The mean square error (MSE) measures the accuracy of a single-valued predictand. It comprises the average square
 * difference between the predictand and verifying observation. Optionally, the MSE may be factored into two-component
 * or three-component decompositions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MeanSquareError<S extends SingleValuedPairs, T extends VectorOutput>
extends
    DecomposableDoubleErrorScore<S, T>
{

    /**
     * The decomposition identifier. See {@link MetricConstants#getDecompositionID()}.
     */

    private final int decompositionID;

    @Override
    public T apply(final S s)
    {

        switch(decompositionID)
        {
            case MetricConstants.NONE:
                return getMSENoDecomp(s);
            case MetricConstants.CR:
            case MetricConstants.LBR:
            case MetricConstants.CR_AND_LBR:
            default:
                throw new MetricCalculationException("The Mean Square Error decomposition is not currently "
                    + "implemented.");
        }

    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanSquareErrorBuilder<S extends SingleValuedPairs, T extends VectorOutput>
    implements MetricBuilder<S, T>
    {
        /**
         * The type of metric decomposition. See {@link MetricConstants#getDecompositionID()}.
         */

        private int decompositionID = MetricConstants.DEFAULT;

        @Override
        public MeanSquareError<S, T> build()
        {
            return new MeanSquareError<>(this);
        }

        public MeanSquareErrorBuilder<S, T> setDecompositionID(final int decompositionID)
        {
            this.decompositionID = decompositionID;
            return this;
        }

    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public int getID()
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
    public int getDecompositionID()
    {
        return decompositionID;
    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    protected MeanSquareError(final MeanSquareErrorBuilder<S, T> b)
    {
        if(!Score.isSupportedDecompositionID(b.decompositionID))
        {
            throw new IllegalStateException("Unrecognized decomposition identifier: " + b.decompositionID);
        }
        this.decompositionID = b.decompositionID;
    }

    /**
     * Returns the Mean Square Error without any decomposition.
     * 
     * @param s the pairs
     * @return the mean square error without decomposition.
     */

    private T getMSENoDecomp(final S s)
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
        return MetricOutputFactory.ofExtendsVectorOutput(result, metOut);
    }

}
