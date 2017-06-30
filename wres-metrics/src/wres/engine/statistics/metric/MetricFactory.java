package wres.engine.statistics.metric;

import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;

/**
 * A factory class for constructing metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricFactory
{

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function.
     */

    public static BrierScore<DiscreteProbabilityPairs, VectorOutput> ofBrierScore()
    {
        return new BrierScore.BrierScoreBuilder<>().build();
    }

    /**
     * Return a {@link BrierScore} function with a prescribed decomposition. See {@link Score#getDecompositionID()}.
     * 
     * @param decompositionID the decompositionID
     * @return a {@link BrierScore} function.
     */

    public static BrierScore<DiscreteProbabilityPairs, VectorOutput> ofBrierScore(final MetricConstants decompositionID)
    {
        return new BrierScore.BrierScoreBuilder<>().setDecompositionID(decompositionID).build();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function.
     */

    public static BrierSkillScore<DiscreteProbabilityPairs, VectorOutput> ofBrierSkillScore()
    {
        return new BrierSkillScore.BrierSkillScoreBuilder<>().build();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function.
     */

    public static ContingencyTable<MulticategoryPairs, MatrixOutput> ofContingencyTable()
    {
        return new ContingencyTable.ContingencyTableBuilder<>().build();
    }

    /**
     * Return a default {@link CriticalSuccessIndex} function.
     * 
     * @return a default {@link CriticalSuccessIndex} function.
     */

    public static CriticalSuccessIndex<DichotomousPairs, ScalarOutput> ofCriticalSuccessIndex()
    {
        return new CriticalSuccessIndex.CriticalSuccessIndexBuilder<>().build();
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function.
     */

    public static EquitableThreatScore<DichotomousPairs, ScalarOutput> ofEquitableThreatScore()
    {
        return new EquitableThreatScore.EquitableThreatScoreBuilder<>().build();
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function.
     */

    public static MeanAbsoluteError<SingleValuedPairs, ScalarOutput> ofMeanAbsoluteError()
    {
        return new MeanAbsoluteError.MeanAbsoluteErrorBuilder<>().build();
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function.
     */

    public static MeanError<SingleValuedPairs, ScalarOutput> ofMeanError()
    {
        return new MeanError.MeanErrorBuilder<>().build();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function.
     */

    public static MeanSquareError<SingleValuedPairs, VectorOutput> ofMeanSquareError()
    {
        return new MeanSquareError.MeanSquareErrorBuilder<>().build();
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function.
     */

    public static MeanSquareErrorSkillScore<SingleValuedPairs, VectorOutput> ofMeanSquareErrorSkillScore()
    {
        return new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>().build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a dichotomous event.
     * 
     * @return a default {@link PeirceSkillScore} function for a dichotomous event.
     */

    public static PeirceSkillScore<DichotomousPairs, ScalarOutput> ofPeirceSkillScore()
    {
        return new PeirceSkillScore.PeirceSkillScoreBuilder<>().build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a multicategory event.
     * 
     * @return a default {@link PeirceSkillScore} function for a multicategory event.
     */

    public static PeirceSkillScore<MulticategoryPairs, ScalarOutput> ofPeirceSkillScoreMulti()
    {
        return new PeirceSkillScore.PeirceSkillScoreMulticategoryBuilder<>().build();
    }

    /**
     * Return a default {@link ProbabilityOfDetection} function.
     * 
     * @return a default {@link ProbabilityOfDetection} function.
     */

    public static ProbabilityOfDetection<DichotomousPairs, ScalarOutput> ofProbabilityOfDetection()
    {
        return new ProbabilityOfDetection.ProbabilityOfDetectionBuilder<>().build();
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function.
     */

    public static ProbabilityOfFalseDetection<DichotomousPairs, ScalarOutput> ofProbabilityOfFalseDetection()
    {
        return new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder<>().build();
    }

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function.
     */

    public static RootMeanSquareError<SingleValuedPairs, ScalarOutput> ofRootMeanSquareError()
    {
        return new RootMeanSquareError.RootMeanSquareErrorBuilder<>().build();
    }

    /**
     * No argument constructor.
     */

    private MetricFactory()
    {
    };

}
