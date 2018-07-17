package wres.datamodel.inputs.pairs;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;

/**
 * Immutable store of verification pairs for two probabilistic variables that are defined for a common, discrete, event.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SafeDiscreteProbabilityPairs extends SafeSingleValuedPairs implements DiscreteProbabilityPairs
{

    @Override
    public DiscreteProbabilityPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        final DataFactory metIn = DefaultDataFactory.getInstance();
        return metIn.ofDiscreteProbabilityPairs(getRawDataForBaseline(),getMetadataForBaseline());
    }     

    /**
     * A {@link DefaultMetricInputBuilder} to build the metric input.
     */

    public static class DiscreteProbabilityPairsBuilder extends SingleValuedPairsBuilder
    {

        @Override
        public SafeDiscreteProbabilityPairs build()
        {
            return new SafeDiscreteProbabilityPairs(this);
        }

    }    
    
    /**
     * Construct the pairs with a builder. TODO: check the inputs are valid probabilities
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    private SafeDiscreteProbabilityPairs(final DiscreteProbabilityPairsBuilder b)
    {
        super(b);
    }

}
