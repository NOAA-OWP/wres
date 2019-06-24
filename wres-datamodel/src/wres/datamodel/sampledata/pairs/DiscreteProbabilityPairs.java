package wres.datamodel.sampledata.pairs;

import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Immutable store of verification pairs for two probabilistic variables that are defined for a common, discrete, 
 * event.
 *  
 * @author james.brown@hydrosolved.com
 */

public class DiscreteProbabilityPairs extends Pairs<DiscreteProbabilityPair>
{

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws SampleDataException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs of( List<DiscreteProbabilityPair> pairs,
                                               SampleMetadata meta )
    {
        return DiscreteProbabilityPairs.of( pairs, null, meta, null, null );
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

    public static DiscreteProbabilityPairs of( DiscreteProbabilityPairs pairs,
                                               SampleMetadata overrideMainMeta,
                                               SampleMetadata overrideBaselineMeta )
    {
        return DiscreteProbabilityPairs.of( pairs.getRawData(),
                                            pairs.getRawDataForBaseline(),
                                            overrideMainMeta,
                                            overrideBaselineMeta,
                                            pairs.getClimatology() );
    }
    
    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @throws SampleDataException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs of( List<DiscreteProbabilityPair> pairs,
                                               SampleMetadata meta,
                                               VectorOfDoubles climatology )
    {
        return DiscreteProbabilityPairs.of( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @throws SampleDataException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs of( List<DiscreteProbabilityPair> pairs,
                                               List<DiscreteProbabilityPair> basePairs,
                                               SampleMetadata mainMeta,
                                               SampleMetadata baselineMeta )
    {
        return DiscreteProbabilityPairs.of( pairs, basePairs, mainMeta, baselineMeta, null );
    }
    
    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @throws SampleDataException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs of( List<DiscreteProbabilityPair> pairs,
                                               List<DiscreteProbabilityPair> basePairs,
                                               SampleMetadata mainMeta,
                                               SampleMetadata baselineMeta,
                                               VectorOfDoubles climatology )
    {
        DiscreteProbabilityPairsBuilder b =
                new DiscreteProbabilityPairsBuilder();

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

    public DiscreteProbabilityPairs getBaselineData()
    {
        if ( Objects.isNull( this.getRawDataForBaseline() ) )
        {
            return null;
        }
        
        return DiscreteProbabilityPairs.of( getRawDataForBaseline(), getMetadataForBaseline() );
    }

    /**
     * A builder to build the metric input.
     */

    public static class DiscreteProbabilityPairsBuilder extends PairsBuilder<DiscreteProbabilityPair>
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
     * @throws SampleDataException if the pairs are invalid
     */

    private DiscreteProbabilityPairs( final DiscreteProbabilityPairsBuilder b )
    {
        super( b );
    }

}
