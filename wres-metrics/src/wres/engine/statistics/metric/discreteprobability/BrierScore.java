package wres.engine.statistics.metric.discreteprobability;

import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.ProbabilityScore;
import wres.engine.statistics.metric.singlevalued.MeanSquareError;

/**
 * <p>
 * The Brier score is the average square difference between a probabilistic predictand and a verifying observation. The
 * verifying observation is also probabilistic, and is typically obtained from the indicator function of a continuous
 * variable (i.e. dichotomous). Optionally, the Brier Score may be factored into two-component or three-component
 * decompositions. By convention, the Brier Score is half of the original Brier Score proposed by Brier (1950):
 * </p>
 * <p>
 * Brier, G. W. (1950) Verification of forecasts expressed in terms of probability. <i> Monthly Weather Review</i>,
 * <b>78</b>, 1-3.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class BrierScore extends DecomposableScore<DiscreteProbabilityPairs>
        implements ProbabilityScore<DiscreteProbabilityPairs, DoubleScoreOutput>
{

    /**
     * Instance of MSE used to compute the BS.
     */
    
    private final MeanSquareError mse;
    
    @Override
    public DoubleScoreOutput apply( DiscreteProbabilityPairs s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new MetricInputException( "Specify non-null input to the '" + this + "'." );
        }

        MetricOutputMetadata metOut = getMetadata( s, s.getRawData().size(), MetricConstants.MAIN, null );

        return DataFactory.ofDoubleScoreOutput( mse.apply( Slicer.toSingleValuedPairs( s ) ).getData(), metOut );
    }

    @Override
    public MetricConstants getID()
    {
        return MetricConstants.BRIER_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    @Override
    public boolean isProper()
    {
        return true;
    }

    @Override
    public boolean isStrictlyProper()
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

    public static class BrierScoreBuilder extends DecomposableScoreBuilder<DiscreteProbabilityPairs>
    {

        @Override
        public BrierScore build() throws MetricParameterException
        {
            return new BrierScore( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricParameterException if one or more parameters is invalid
     */

    BrierScore( final BrierScoreBuilder builder ) throws MetricParameterException
    {
        super( builder );
        
        mse = MetricFactory.ofMeanSquareError();
    }

}
