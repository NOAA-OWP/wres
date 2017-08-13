package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DatasetIdentifier;
import wres.datamodel.metric.MetricConstants;
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
public class MeanSquareErrorSkillScore<S extends SingleValuedPairs> extends MeanSquareError<S>
{

    @Override
    public VectorOutput apply(final SingleValuedPairs s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        //TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        //template as the componentID in the metadata
        DatasetIdentifier baselineIdentifier = null;
        double numerator = getSumOfSquareError(s);
        double denominator = 0.0;
        if(s.hasBaseline())
        {
            denominator = getSumOfSquareError(s.getBaselineData());
            baselineIdentifier = s.getMetadataForBaseline().getIdentifier();
        }
        else 
        {
            DataFactory d = getDataFactory();
            double meanRight = FunctionFactory.mean().applyAsDouble(d.vectorOf(d.getSlicer().getRightSide(s)));
            for(PairOfDoubles next : s.getData()) {
                denominator+=next.getItemOne()-meanRight;
            }
        }
        final double[] result = new double[]{
            FunctionFactory.skill().applyAsDouble(numerator,
                                                  denominator)};
        //Metadata
        final MetricOutputMetadata metOut =
                                          getMetadata(s,
                                                      s.getData().size(),
                                                      MetricConstants.MAIN,
                                                      baselineIdentifier);        
        return getDataFactory().ofVectorOutput(result, metOut);
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
     * A {@link MetricBuilder} to build the metric.
     */

    protected static class MeanSquareErrorSkillScoreBuilder<S extends SingleValuedPairs>
    extends
        MeanSquareErrorBuilder<S>
    {

        @Override
        protected MeanSquareErrorSkillScore<S> build()
        {
            return new MeanSquareErrorSkillScore<>(this);
        }

    }

    /**
     * Prevent direct construction.
     * 
     * @param builder the builder
     */

    protected MeanSquareErrorSkillScore(final MeanSquareErrorSkillScoreBuilder<S> builder)
    {
        super(builder);
    }

}
