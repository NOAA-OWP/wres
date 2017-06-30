package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricInputException;
import wres.datamodel.metric.MetricInputFactory;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;

/**
 * The Mean Square Error (MSE) Skill Score (SS) measures the reduction in MSE associated with one set of predictions
 * when compared to another. The MSE-SS is equivalent to the Nash-Sutcliffe Efficiency. The perfect MSE-SS is 1.0.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class MeanSquareErrorSkillScore<S extends SingleValuedPairs, T extends VectorOutput>
extends
    MeanSquareError<S, T>
{

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        if(!s.hasBaseline())
        {
            throw new MetricInputException("Specify a non-null baseline for the '" + toString() + "'.");
        }
        //TODO: implement any required decompositions, based on the instance parameters  
        //Metadata
        final Metadata metIn = s.getMetadata();
        final MetricOutputMetadata metOut = MetadataFactory.getMetadata(metIn.getSampleSize(),
                                                                        metIn.getDimension(),
                                                                        getID(),
                                                                        MetricConstants.MAIN,
                                                                        metIn.getID(),
                                                                        s.getMetadataForBaseline().getID());
        final VectorOutput numerator = super.apply(s);
        final VectorOutput denominator = super.apply(MetricInputFactory.ofExtendsSingleValuedPairs(s.getDataForBaseline(),
                                                                                                   metOut));
        final double[] result = new double[]{
            FunctionFactory.skill().applyAsDouble(numerator.getData().getDoubles()[0],
                                                  denominator.getData().getDoubles()[0])};
        return MetricOutputFactory.ofExtendsVectorOutput(result,metOut);
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    public static class MeanSquareErrorSkillScoreBuilder<S extends SingleValuedPairs, T extends VectorOutput>
    extends
        MeanSquareErrorBuilder<S, T>
    {

        @Override
        public MeanSquareErrorSkillScore<S, T> build()
        {
            return new MeanSquareErrorSkillScore<>(this);
        }

    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }
    
    @Override
    public boolean hasRealUnits()
    {
        return false;
    }        

    /**
     * Prevent direct construction.
     * 
     * @param b the builder
     */

    protected MeanSquareErrorSkillScore(final MeanSquareErrorSkillScoreBuilder<S, T> b)
    {
        super(b);
    }

}
