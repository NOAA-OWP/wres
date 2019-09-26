package wres.datamodel.sampledata.pairs;

import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;

/**
 * Immutable store of verification pairs that comprise two single-valued, continuous numerical, variables. The 
 * single-valued variables are not necessarily deterministic (i.e. they may be probabilistic), but they do comprise 
 * single values, rather than multiple values.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SingleValuedPairs extends Pairs<SingleValuedPair>
{

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param meta the metadata
     * @return the pairs
     * @throws SampleDataException if the inputs are invalid
     */

    public static SingleValuedPairs of( List<SingleValuedPair> pairs, SampleMetadata meta )
    {
        return SingleValuedPairs.of( pairs, null, meta, null, null );
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

    public static SingleValuedPairs of( SingleValuedPairs pairs,
                                        SampleMetadata overrideMainMeta,
                                        SampleMetadata overrideBaselineMeta )
    {
        return SingleValuedPairs.of( pairs.getRawData(),
                                     pairs.getRawDataForBaseline(),
                                     overrideMainMeta,
                                     overrideBaselineMeta,
                                     pairs.getClimatology() );
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws SampleDataException if the inputs are invalid
     */

    public static SingleValuedPairs of( List<SingleValuedPair> pairs,
                                        SampleMetadata meta,
                                        VectorOfDoubles climatology )
    {
        return SingleValuedPairs.of( pairs, null, meta, null, climatology );
    }

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws SampleDataException if the inputs are invalid
     */

    public static SingleValuedPairs of( List<SingleValuedPair> pairs,
                                        List<SingleValuedPair> basePairs,
                                        SampleMetadata mainMeta,
                                        SampleMetadata baselineMeta )
    {
        return SingleValuedPairs.of( pairs, basePairs, mainMeta, baselineMeta, null );
    }

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws SampleDataException if the inputs are invalid
     */

    public static SingleValuedPairs of( List<SingleValuedPair> pairs,
                                        List<SingleValuedPair> basePairs,
                                        SampleMetadata mainMeta,
                                        SampleMetadata baselineMeta,
                                        VectorOfDoubles climatology )
    {
        SingleValuedPairsBuilder b =
                new SingleValuedPairsBuilder();

        b.addData( pairs ).setMetadata( mainMeta ).setClimatology( climatology );

        if ( Objects.nonNull( basePairs ) )
        {
            b.addDataForBaseline( basePairs ).setMetadataForBaseline( baselineMeta );
        }

        return b.build();
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
     * Returns the baseline data as a {@link SampleData} or null if no baseline is defined. 
     * 
     * @return the baseline
     */
    @Override
    public SingleValuedPairs getBaselineData()
    {
        if ( ! this.hasBaseline() )
        {
            return null;
        }
        
        return SingleValuedPairs.of( this.getRawDataForBaseline(), this.getMetadataForBaseline() );
    }

    /**
     * A builder to build the metric input.
     */

    public static class SingleValuedPairsBuilder extends PairsBuilder<SingleValuedPair>
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
     * @throws SampleDataException if the pairs are invalid
     */

    SingleValuedPairs( final SingleValuedPairsBuilder b )
    {
        super( b );
    }

}
