package wres.datamodel.inputs.pairs;

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;

/**
 * Immutable store of verification pairs that comprise two single-valued, continuous numerical, variables. The 
 * single-valued variables are not necessarily deterministic (i.e. they may be probabilistic), but they do comprise 
 * single values, rather than multiple values.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SingleValuedPairs extends BasicPairs<SingleValuedPair>
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined. 
     * 
     * @return the baseline
     */
    @Override
    public SingleValuedPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        return DataFactory.ofSingleValuedPairs( this.getRawDataForBaseline(), this.getMetadataForBaseline() );
    }

    /**
     * A builder to build the metric input.
     */

    public static class SingleValuedPairsBuilder extends BasicPairsBuilder<SingleValuedPair>
    {

        @Override
        public SingleValuedPairs build()
        {
            return new SingleValuedPairs( this );
        }
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SingleValuedPairs( final SingleValuedPairsBuilder b )
    {
        super( b );
    }

}
