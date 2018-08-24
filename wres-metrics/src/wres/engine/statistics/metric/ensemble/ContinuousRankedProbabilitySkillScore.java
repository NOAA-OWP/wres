package wres.engine.statistics.metric.ensemble;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.statistics.DoubleScoreOutput;
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
 */
public class ContinuousRankedProbabilitySkillScore extends ContinuousRankedProbabilityScore
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static ContinuousRankedProbabilitySkillScore of()
    {
        return new ContinuousRankedProbabilitySkillScore();
    }

    /**
     * Returns an instance.
     * 
     * @param decompositionId the decomposition identifier
     * @return an instance
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    public static ContinuousRankedProbabilitySkillScore of( ScoreOutputGroup decompositionId )
            throws MetricParameterException
    {
        return new ContinuousRankedProbabilitySkillScore( decompositionId );
    }

    @Override
    public DoubleScoreOutput apply( EnsemblePairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }
        if ( !s.hasBaseline() )
        {
            throw new MetricInputException( "Specify a non-null baseline for the '" + this + "'." );
        }
        //CRPSS, currently without decomposition
        //TODO: implement the decomposition
        double numerator = super.apply( s ).getComponent( MetricConstants.MAIN ).getData();
        double denominator = super.apply( s.getBaselineData() ).getComponent( MetricConstants.MAIN ).getData();
        double result = FunctionFactory.skill().applyAsDouble( numerator, denominator );

        //Metadata
        DatasetIdentifier baselineIdentifier = s.getMetadataForBaseline().getIdentifier();
        MetricOutputMetadata metOut = MetricOutputMetadata.of( s.getMetadata(),
                                                          this.getID(),
                                                          MetricConstants.MAIN,
                                                          this.hasRealUnits(),
                                                          s.getRawData().size(),
                                                          baselineIdentifier );
        return DoubleScoreOutput.of( result, metOut );
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
     * Hidden constructor.
     */

    private ContinuousRankedProbabilitySkillScore()
    {
        super();
    }

    /**
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     * @throws MetricParameterException if one or more parameters is invalid 
     */

    private ContinuousRankedProbabilitySkillScore( ScoreOutputGroup decompositionId ) throws MetricParameterException
    {
        super( decompositionId );
    }

}
