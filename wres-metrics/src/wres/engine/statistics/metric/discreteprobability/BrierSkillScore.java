package wres.engine.statistics.metric.discreteprobability;

import java.util.Objects;

import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;

/**
 * <p>
 * The Brier Skill Score (SS) measures the reduction in the {@link BrierScore} (i.e. probabilistic Mean Square Error)
 * associated with one set of predictions when compared to another. The BSS is analogous to the
 * {@link MeanSquareErrorSkillScore} or the Nash-Sutcliffe Efficiency for a single-valued input. The perfect BSS is 1.0.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class BrierSkillScore extends BrierScore
{

    /**
     * Instance of MSE-SS used to compute the BSS.
     */

    private final MeanSquareErrorSkillScore msess;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static BrierSkillScore of()
    {
        return new BrierSkillScore();
    }

    @Override
    public DoubleScoreOutput apply( DiscreteProbabilityPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        DatasetIdentifier baselineIdentifier = null;
        if ( s.hasBaseline() )
        {
            baselineIdentifier = s.getMetadataForBaseline().getIdentifier();
        }

        MetricOutputMetadata metOut =
                MetricOutputMetadata.of( s.getMetadata(),
                                    this.getID(),
                                    MetricConstants.MAIN,
                                    this.hasRealUnits(),
                                    s.getRawData().size(),
                                    baselineIdentifier );

        return DoubleScoreOutput.of( msess.apply( Slicer.toSingleValuedPairs( s ) ).getData(), metOut );
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
     * Hidden constructor.
     */

    private BrierSkillScore()
    {
        super();

        msess = MeanSquareErrorSkillScore.of();
    }

}
