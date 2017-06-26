package wres.engine.statistics.metric.inputs;

import wres.datamodel.metric.MetricInputBuilder;

/**
 * Immutable store of verification pairs that comprise probabilistic observations and predictions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class DiscreteProbabilityPairs extends SingleValuedPairs
{

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    public static class DiscreteProbabilityPairsBuilder extends SingleValuedPairsBuilder
    {

        @Override
        public DiscreteProbabilityPairs build()
        {
            return new DiscreteProbabilityPairs(this);
        }

    }

    /**
     * Construct the probability pairs with a builder. TODO placeholder for check the inputs are valid probabilities
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    private DiscreteProbabilityPairs(final DiscreteProbabilityPairsBuilder b)
    {
        super(b);
    }

}
