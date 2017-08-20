package wres.datamodel;

import java.util.Objects;

import wres.datamodel.Threshold.Operator;

/**
 * Concrete implementation of a {@link Probability}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeProbabilityThreshold extends SafeThreshold implements ProbabilityThreshold
{
    /**
     * Construct the threshold.
     *
     * @param threshold the threshold
     * @param thresholdUpper the upper threshold or null
     * @param condition the condition
     */

    protected SafeProbabilityThreshold(final Double threshold, final Double thresholdUpper, final Operator condition)
    {
        super(threshold, thresholdUpper, condition);
        //Check inputs are within bounds
        if(threshold < 0.0 || threshold > 1.0)
        {
            throw new IllegalArgumentException("The threshold probability is out of bounds [0,1]: " + threshold);
        }
        if(Objects.nonNull(thresholdUpper) && (thresholdUpper < 0.0 || thresholdUpper > 1.0))
        {
            throw new IllegalArgumentException("The upper threshold probability is out of bounds [0,1]: "
                + thresholdUpper);
        }
    }
}
