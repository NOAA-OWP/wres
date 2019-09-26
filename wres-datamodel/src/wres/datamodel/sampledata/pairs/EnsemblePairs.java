package wres.datamodel.sampledata.pairs;

import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Immutable store of {@link EnsemblePair} where the left side is a single value and the right side 
 * is an ensemble of values. Metrics should anticipate the possibility of an inconsistent number of ensemble members 
 * in each pair (e.g. due to missing values).
 * 
 * @author james.brown@hydrosolved.com
 */
public class EnsemblePairs extends Pairs<EnsemblePair>
{

    /**
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @return the pairs
     * @throws SampleDataException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs, SampleMetadata meta )
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
     * @throws SampleDataException if the inputs are invalid
     */

    public static EnsemblePairs of( EnsemblePairs pairs,
                                    SampleMetadata overrideMainMeta,
                                    SampleMetadata overrideBaselineMeta )
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
     * @throws SampleDataException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs,
                                    SampleMetadata meta,
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
     * @throws SampleDataException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs,
                                    List<EnsemblePair> basePairs,
                                    SampleMetadata mainMeta,
                                    SampleMetadata baselineMeta )
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
     * @throws SampleDataException if the inputs are invalid
     */

    public static EnsemblePairs of( List<EnsemblePair> pairs,
                                    List<EnsemblePair> basePairs,
                                    SampleMetadata mainMeta,
                                    SampleMetadata baselineMeta,
                                    VectorOfDoubles climatology )
    {
        EnsemblePairsBuilder b =
                new EnsemblePairsBuilder();

        b.addData( pairs ).setMetadata( mainMeta ).setClimatology( climatology );

        if ( Objects.nonNull( basePairs ) )
        {
            b.addDataForBaseline( basePairs ).setMetadataForBaseline( baselineMeta );
        }

        return b.build();
    }

    /**
     * Returns the baseline data as a {@link SampleData} or null if no baseline is defined.
     * 
     * @return the baseline
     */
    @Override
    public EnsemblePairs getBaselineData()
    {
        if ( !this.hasBaseline() )
        {
            return null;
        }
        
        return EnsemblePairs.of( this.getRawDataForBaseline(), this.getMetadataForBaseline() );
    }

    @Override
    public boolean hasBaseline()
    {
        return Objects.nonNull( this.getRawDataForBaseline() );
    }

    @Override
    public boolean hasClimatology()
    {
        return Objects.nonNull( this.getClimatology() );
    }
    
    /**
     * A builder to build the metric input.
     */

    public static class EnsemblePairsBuilder extends PairsBuilder<EnsemblePair>
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
     * @throws SampleDataException if the pairs are invalid
     */

    EnsemblePairs( final EnsemblePairsBuilder b )
    {
        super( b );
    }

}
