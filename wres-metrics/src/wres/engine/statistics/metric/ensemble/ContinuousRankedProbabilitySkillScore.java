package wres.engine.statistics.metric.ensemble;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.VectorOutput;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * <p>
 * The Continuous Ranked Probability Skill Score (CRPSS) measures the reduction in the 
 * {@link ContinuousRankedProbabilityScore} associated with one set of predictions when compared to another. The perfect
 * score is 1.0. 
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ContinuousRankedProbabilitySkillScore extends ContinuousRankedProbabilityScore
{

    @Override
    public VectorOutput apply( EnsemblePairs s )
    {
        if(Objects.isNull(s))
        {
            throw new MetricInputException("Specify non-null input to the '"+this+"'.");
        }
        if ( !s.hasBaseline() )
        {
            throw new MetricInputException( "Specify a non-null baseline for the '" + this + "'." );
        }
        //CRPSS, currently without decomposition
        //TODO: implement the decomposition
        double numerator = super.apply( s ).getValue( MetricConstants.MAIN );
        double denominator = super.apply( s.getBaselineData() ).getValue( MetricConstants.MAIN );
        final double[] result = new double[] { FunctionFactory.skill().applyAsDouble( numerator, denominator ) };

        //Metadata
        final MetricOutputMetadata metOut = getMetadata( s, s.getData().size(), MetricConstants.NONE, null );
        return getDataFactory().ofVectorOutput( result, metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE;
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

    public static class CRPSSBuilder extends CRPSBuilder
    {
        @Override
        protected ContinuousRankedProbabilitySkillScore build() throws MetricParameterException
        {
            return new ContinuousRankedProbabilitySkillScore( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    private ContinuousRankedProbabilitySkillScore( final CRPSSBuilder builder ) throws MetricParameterException
    {
        super( builder );
    }

}
