package wres.datamodel.inputs.pairs;

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;

/**
 * Immutable store of verification pairs for two probabilistic variables that are defined for a common, discrete, 
 * event.
 *  
 * @author james.brown@hydrosolved.com
 */

public class DiscreteProbabilityPairs extends SingleValuedPairs
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    public DiscreteProbabilityPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        return DataFactory.ofDiscreteProbabilityPairs( getRawDataForBaseline(), getMetadataForBaseline() );
    }

    /**
     * A builder to build the metric input.
     */

    public static class DiscreteProbabilityPairsBuilder extends SingleValuedPairsBuilder
    {

        @Override
        public DiscreteProbabilityPairs build()
        {
            return new DiscreteProbabilityPairs( this );
        }

    }

    /**
     * Construct the pairs with a builder. TODO: check the inputs are valid probabilities
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    private DiscreteProbabilityPairs( final DiscreteProbabilityPairsBuilder b )
    {
        super( b );
    }

}
