package wres.engine.statistics.metric.inputs;

import java.util.List;

import wres.datamodel.PairOfDoubles;

/**
 * Class for storing verification pairs that comprise probabilistic observations and predictions.
 * 
 * @author james.brown@hydrosolved.com
 */
public class DiscreteProbabilityPairs extends SingleValuedPairs
{

    /**
     * Construct the discrete probability input without any pairs for a baseline. Throws an exception if the pairs are
     * null, empty, incomplete, or out of bounds.
     * 
     * @param pairs the discrete probability pairs
     * @throws MetricInputException if the pairs are null, empty, incomplete, or out of bounds
     */

    protected DiscreteProbabilityPairs(final List<PairOfDoubles> pairs)
    {
        super(pairs, null);
        // TODO Auto-generated constructor stub
        // TBD: check bounds of probabilities
    }

    /**
     * Construct the probability input with a baseline. Throws an exception if the pairs are null, empty, incomplete or
     * out of bounds or if the baseline is empty, incomplete or out of bounds. The baseline may be null.
     * 
     * @param pairs the discrete probability pairs
     * @param basePairs the baseline pairs
     * @throws MetricInputException if the pairs are null, empty, incomplete, or out of bounds
     */

    protected DiscreteProbabilityPairs(final List<PairOfDoubles> pairs, final List<PairOfDoubles> basePairs)
    {
        super(pairs, basePairs, null);
        // TODO Auto-generated constructor stub
        // TBD: check bounds of probabilities
    }

}
