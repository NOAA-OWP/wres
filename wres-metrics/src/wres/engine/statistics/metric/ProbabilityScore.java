package wres.engine.statistics.metric;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.ScoreOutput;

/**
 * <p>
 * Identifies a scoring rule that operates on probabilistic inputs.
 * </p>
 * <p>
 * Propriety is an important attribute of scores over probabilistic predictions, and indicates whether there is scope to
 * "hedge" by issuing predictions that deviate from the expected outcome, simply to maximize the score. Since hedging is
 * undesirable (it encourages predictions that are untruthful), scores that cannot be hedged are preferred. These are
 * known as "proper" scores when the expected score is optimized by issuing a truthful prediction and "strictly proper"
 * when the optimal score is <b>uniquely</b> achieved for the truthful prediction. For further details, see:
 * </p>
 * <p>
 * Gneiting, T. and Raftery, A.E. (2007) Strictly proper scoring rules: prediction and estimation. <i>Journal of the
 * American Statistical Association</i>, <b>102</b>(477), 359-378.
 * </p>
 * <p>
 * The propriety of a scoring rule is not guaranteed, and an otherwise proper scoring rule may be hedged for a subset of
 * data. For example, when conditioning on observed amount, a proper scoring rule will favour models that overestimate
 * large observations, otherwise known as the "forecaster's dilemma" (see: https://arxiv.org/abs/1512.09244v1).
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface ProbabilityScore<S extends MetricInput<?>, T extends ScoreOutput<?,T>> extends Score<S,T>
{

    /**
     * Returns true if the the scoring rule is, in principle, a proper scoring rule. A probabilistic scoring rule is
     * proper if it cannot be hedged.
     * 
     * @return true if the scoring rule is proper
     */

    boolean isProper();

    /**
     * Returns true if {@link #isProper()} returns true and the optimal score is unique.
     * 
     * @return true if the scoring rule is, in principle, strictly proper
     */

    boolean isStrictlyProper();

}
