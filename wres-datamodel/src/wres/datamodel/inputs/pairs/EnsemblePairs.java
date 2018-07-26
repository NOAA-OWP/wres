package wres.datamodel.inputs.pairs;

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;

/**
 * Immutable store of {@link EnsemblePair} where the left side is a single value and the right side 
 * is an ensemble of values. Metrics should anticipate the possibility of an inconsistent number of ensemble members 
 * in each pair (e.g. due to missing values).
 * 
 * @author james.brown@hydrosolved.com
 */
public class EnsemblePairs extends BasicPairs<EnsemblePair>
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */
    @Override
    public EnsemblePairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        return DataFactory.ofEnsemblePairs( this.getRawDataForBaseline(), this.getMetadataForBaseline() );
    }

    /**
     * A builder to build the metric input.
     */

    public static class EnsemblePairsBuilder extends BasicPairsBuilder<EnsemblePair>
    {

        @Override
        public EnsemblePairs build()
        {
            return new EnsemblePairs( this );
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    EnsemblePairs( final EnsemblePairsBuilder b )
    {
        super( b );
    }

}
