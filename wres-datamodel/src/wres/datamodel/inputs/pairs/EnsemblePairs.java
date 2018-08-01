package wres.datamodel.inputs.pairs;

import java.util.List;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;

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
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs, Metadata meta )
    {
        return EnsemblePairs.of( pairs, null, meta, null, null );
    }

    /**
     * Construct the input from an existing input and override metadata.
     * 
     * @param pairs the existing pairs
     * @param overrideMainMeta the metadata for the main pairs
     * @param overrideBaselineMeta the metadata for the baseline pairs (may be null, if the baseline pairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static EnsemblePairs of( EnsemblePairs pairs,
                                    Metadata overrideMainMeta,
                                    Metadata overrideBaselineMeta )
    {
        return EnsemblePairs.of( pairs.getRawData(),
                                 pairs.getRawDataForBaseline(),
                                 overrideMainMeta,
                                 overrideBaselineMeta,
                                 pairs.getClimatology() );
    }

    /**
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs,
                                    Metadata meta,
                                    VectorOfDoubles climatology )
    {
        return EnsemblePairs.of( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the ensemble input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs,
                                    List<EnsemblePair> basePairs,
                                    Metadata mainMeta,
                                    Metadata baselineMeta )
    {
        return EnsemblePairs.of( pairs, basePairs, mainMeta, baselineMeta, null );
    }

    /**
     * Construct the ensemble input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs,
                                    List<EnsemblePair> basePairs,
                                    Metadata mainMeta,
                                    Metadata baselineMeta,
                                    VectorOfDoubles climatology )
    {
        EnsemblePairsBuilder b = new EnsemblePairsBuilder();
        return (EnsemblePairs) b.setMetadata( mainMeta )
                                .addData( pairs )
                                .addDataForBaseline( basePairs )
                                .setMetadataForBaseline( baselineMeta )
                                .setClimatology( climatology )
                                .build();
    }

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
        return EnsemblePairs.of( this.getRawDataForBaseline(), this.getMetadataForBaseline() );
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
