package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MatrixOutput;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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

    public static BrierScore<DiscreteProbabilityPairs, ScalarOutput> ofBrierScoreNoDecomp()
    {
        return new BrierScore<DiscreteProbabilityPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function.
     */

    public static BrierSkillScore<DiscreteProbabilityPairs, ScalarOutput> ofBrierSkillScoreNoDecomp()
    {
        return new BrierSkillScore<DiscreteProbabilityPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function.
     */

    public static ContingencyTable<MulticategoryPairs, MatrixOutput> ofContingencyTable()
    {
        return new ContingencyTable<MulticategoryPairs, MatrixOutput>();
    }

    /**
     * Return a default {@link CriticalSuccessIndex} function.
     * 
     * @return a default {@link CriticalSuccessIndex} function.
     */

    public static CriticalSuccessIndex<DichotomousPairs, ScalarOutput> ofCriticalSuccessIndex()
    {
        return new CriticalSuccessIndex<DichotomousPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function.
     */

    public static EquitableThreatScore<DichotomousPairs, ScalarOutput> ofEquitableThreatScore()
    {
        return new EquitableThreatScore<DichotomousPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function.
     */

    public static MeanAbsoluteError<SingleValuedPairs, ScalarOutput> ofMeanAbsoluteError()
    {
        return new MeanAbsoluteError<SingleValuedPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function.
     */

    public static MeanError<SingleValuedPairs, ScalarOutput> ofMeanError()
    {
        return new MeanError<SingleValuedPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function.
     */

    public static MeanSquareError<SingleValuedPairs, ScalarOutput> ofMeanSquareError()
    {
        return new MeanSquareError<SingleValuedPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function.
     */

    public static MeanSquareErrorSkillScore<SingleValuedPairs, ScalarOutput> ofMeanSquareErrorSkillScore()
    {
        return new MeanSquareErrorSkillScore<SingleValuedPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a dichotomous event.
     * 
     * @return a default {@link PeirceSkillScore} function for a dichotomous event.
     */

    public static PeirceSkillScore<DichotomousPairs, ScalarOutput> ofPeirceSkillScore()
    {
        return new PeirceSkillScore<DichotomousPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link ProbabilityOfDetection} function.
     * 
     * @return a default {@link ProbabilityOfDetection} function.
     */

    public static ProbabilityOfDetection<DichotomousPairs, ScalarOutput> ofProbabilityOfDetection()
    {
        return new ProbabilityOfDetection<DichotomousPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function.
     */

    public static ProbabilityOfFalseDetection<DichotomousPairs, ScalarOutput> ofProbabilityOfFalseDetection()
    {
        return new ProbabilityOfFalseDetection<DichotomousPairs, ScalarOutput>();
    }

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function.
     */

    public static RootMeanSquareError<SingleValuedPairs, ScalarOutput> ofRootMeanSquareError()
    {
        return new RootMeanSquareError<SingleValuedPairs, ScalarOutput>();
    }

    /**
     * No argument constructor.
     */

    private MetricFactory()
    {
    };

}
