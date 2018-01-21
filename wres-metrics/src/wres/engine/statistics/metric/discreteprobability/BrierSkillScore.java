package wres.engine.statistics.metric.discreteprobability;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.ProbabilityScore;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;

/**
 * <p>
 * The Brier Skill Score (SS) measures the reduction in the {@link BrierScore} (i.e. probabilistic Mean Square Error)
 * associated with one set of predictions when compared to another. The BSS is analogous to the
 * {@link MeanSquareErrorSkillScore} or the Nash-Sutcliffe Efficiency for a single-valued input. The perfect BSS is 1.0.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class BrierSkillScore extends MeanSquareErrorSkillScore<DiscreteProbabilityPairs>
        implements ProbabilityScore<DiscreteProbabilityPairs,MultiValuedScoreOutput>
{

    @Override
    public MultiValuedScoreOutput apply( final DiscreteProbabilityPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        //Explicit baseline
        if ( s.hasBaseline() )
        {
            SingleValuedPairs input = s;
            return super.apply( (DiscreteProbabilityPairs) input );
        }
        //Climatological baseline
        else
        {
            DataFactory d = getDataFactory();
            //Bernoulli R.V. with probability p 
            double p = FunctionFactory.mean().applyAsDouble( d.vectorOf( d.getSlicer().getLeftSide( s ) ) );
            double climP = p * ( 1.0 - p );
            final double[] result = new double[1];
            if ( climP > 0 )
            {
                result[0] =
                        FunctionFactory.skill().applyAsDouble( getSumOfSquareError( s ) / s.getData().size(),
                                                               p * ( 1.0 - p ) );
            }
            else
            {
                result[0] = Double.NaN;
            }
            //Metadata
            final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.NONE, null );
            return getDataFactory().ofMultiValuedScoreOutput( result, metOut );
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

    public static class BrierSkillScoreBuilder extends MeanSquareErrorSkillScoreBuilder<DiscreteProbabilityPairs>
    {

        @Override
        public BrierSkillScore build() throws MetricParameterException
        {
            return new BrierSkillScore( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private BrierSkillScore( final BrierSkillScoreBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
