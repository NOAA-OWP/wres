package wres.datamodel.metric;

/**
 * Immutable store of verification pairs for two probabilistic variables that are defined for a common, discrete, event.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeDiscreteProbabilityPairs extends SafeSingleValuedPairs implements DiscreteProbabilityPairs
{

    @Override
    public DiscreteProbabilityPairs getBaselineData()
    {
        final MetricInputFactory metIn = DefaultMetricInputFactory.getInstance();
        return metIn.ofDiscreteProbabilityPairs(getDataForBaseline(),getMetadataForBaseline());
    }     

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    static class DiscreteProbabilityPairsBuilder extends SingleValuedPairsBuilder
    {

        @Override
        public SafeDiscreteProbabilityPairs build()
        {
            return new SafeDiscreteProbabilityPairs(this);
        }

    }    
    
    /**
     * Construct the probability pairs with a builder. TODO placeholder for check the inputs are valid probabilities
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    private SafeDiscreteProbabilityPairs(final DiscreteProbabilityPairsBuilder b)
    {
        super(b);
    }

}
