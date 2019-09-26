package wres.engine.statistics.metric.discreteprobability;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.Probability;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.DecomposableScore;
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
public class BrierScore extends DecomposableScore<SampleData<Pair<Probability, Probability>>>
        implements ProbabilityScore<SampleData<Pair<Probability, Probability>>, DoubleScoreStatistic>
{

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static BrierScore of()
    {
        return new BrierScore();
    }

    /**
     * Instance of MSE used to compute the BS.
     */

    private final MeanSquareError mse;

    @Override
    public DoubleScoreStatistic apply( SampleData<Pair<Probability, Probability>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        StatisticMetadata metOut = StatisticMetadata.of( s.getMetadata(),
                                                         this.getID(),
                                                         MetricConstants.MAIN,
                                                         this.hasRealUnits(),
                                                         s.getRawData().size(),
                                                         null );

        // Transform probabilities to double values
        SampleData<Pair<Double, Double>> transformed =
                Slicer.transform( s,
                                  pair -> Pair.of( pair.getLeft().getProbability(),
                                                   pair.getRight().getProbability() ) );

        return DoubleScoreStatistic.of( mse.apply( transformed ).getData(), metOut );
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
     * Hidden constructor.
     * 
     * @param decompositionId the decomposition identifier
     */

    BrierScore()
    {
        super();

        mse = MeanSquareError.of();
    }

}
