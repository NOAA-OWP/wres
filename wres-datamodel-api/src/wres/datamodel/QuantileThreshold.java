package wres.datamodel;

/**
 * A threshold that represents a quantile of a probability distribution for which a corresponding probability must be
 * provided. For thresholds that comprise a {@link Operator#BETWEEN} condition, specify a probability for each of the
 * lower and upper bounds.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface QuantileThreshold extends Threshold
{

    /**
     * Returns the probability associated with the {@link #getThreshold()}, which may comprise the lower bound of a
     * {@link Operator#BETWEEN}
     * 
     * @return a probability
     */

    Double getThresholdProbability();

    /**
     * Returns the probability associated with the {@link #getThresholdUpper()} or null if no upper bound is defined.
     * 
     * @return the upper threshold probability or null
     */

    Double getThresholdUpperProbability();

}
