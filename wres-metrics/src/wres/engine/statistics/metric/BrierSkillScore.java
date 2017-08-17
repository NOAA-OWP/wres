package wres.engine.statistics.metric;

import java.util.Objects;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.metric.MetricInputException;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;

/**
 * <p>
 * The Brier Skill Score (SS) measures the reduction in the Brier Score (i.e. probabilistic Mean Square Error)
 * associated with one set of predictions when compared to another. The BSS is analogous to the
 * {@link MeanSquareErrorSkillScore} or the Nash-Sutcliffe Efficiency for a single-valued input. The perfect BSS is 1.0.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class BrierSkillScore extends MeanSquareErrorSkillScore<DiscreteProbabilityPairs>
implements ProbabilityScore
{

    @Override
    public VectorOutput apply(final DiscreteProbabilityPairs s)
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        //Explicit baseline
        if(s.hasBaseline())
        {
            SingleValuedPairs input = s;
            return super.apply(input);
        }
        //Climatological baseline
        else
        {
            DataFactory d = getDataFactory();
            //Bernoulli R.V. with probability p 
            double p = FunctionFactory.mean().applyAsDouble(d.vectorOf(d.getSlicer().getLeftSide(s)));
            final double[] result = new double[]{
                FunctionFactory.skill().applyAsDouble(getSumOfSquareError(s) / s.size(), p * (1.0 - p))};
            //Metadata
            final MetricOutputMetadata metOut = getMetadata(s, s.getData().size(), MetricConstants.MAIN, null);
            return getDataFactory().ofVectorOutput(result, metOut);
        }
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BRIER_SKILL_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return true;
    }

    @Override
    public boolean isProper()
    {
        return false;
    }

    @Override
    public boolean isStrictlyProper()
    {
        return false;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * A {@link MetricBuilder} to build the metric.
     */

    static class BrierSkillScoreBuilder extends MeanSquareErrorSkillScoreBuilder<DiscreteProbabilityPairs>
    {

        @Override
        protected BrierSkillScore build()
        {
            return new BrierSkillScore(this);
        }

        @Override
        protected BrierSkillScoreBuilder setDecompositionID(final MetricDecompositionGroup decompositionID)
        {
            super.setDecompositionID(decompositionID);
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private BrierSkillScore(final BrierSkillScoreBuilder builder)
    {
        super(builder);
    }

}
