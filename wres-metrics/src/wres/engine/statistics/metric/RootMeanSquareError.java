package wres.engine.statistics.metric;

import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
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
public final class RootMeanSquareError<S extends SingleValuedPairs, T extends ScalarOutput>
extends
    DoubleErrorScore<S, T>
{

    @Override
    public T apply(final S t)
    {
        //Metadata
        final Metadata metIn = t.getMetadata();
        final MetricOutputMetadata metOut = MetadataFactory.getMetadata(metIn.getSampleSize(),
                                                                        metIn.getDimension(),
                                                                        getID(),
                                                                        MetricConstants.MAIN,
                                                                        metIn.getID(),
                                                                        null);
        return MetricOutputFactory.ofExtendsScalarOutput(Math.pow(super.apply(t).getData(), 0.5),
                                                         metOut);
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class RootMeanSquareErrorBuilder<S extends SingleValuedPairs, T extends ScalarOutput>
    extends
        DoubleErrorScoreBuilder<S, T>
    {

        @Override
        public RootMeanSquareError<S, T> build()
        {
            return new RootMeanSquareError<>(this);
        }

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
    public MetricConstants getDecompositionID()
    {
        return MetricConstants.NONE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * Hidden constructor.
     * 
     * @param b the builder
     */

    private RootMeanSquareError(final RootMeanSquareErrorBuilder<S, T> b)
    {
        super(b.setErrorFunction(FunctionFactory.squareError()));
    }

}
