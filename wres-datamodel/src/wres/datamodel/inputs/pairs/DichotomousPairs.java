package wres.datamodel.inputs.pairs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;

/**
 * Immutable store of verification pairs associated with a dichotomous input, i.e. a single event whose outcome is
 * recorded as occurring (true) or not occurring (false). The event is not defined as part of the input. A dichotomous
 * pair is be encoded with a single indicator.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DichotomousPairs extends MulticategoryPairs
{

    /**
     * Construct the dichotomous input from atomic {@link DichotomousPair} without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */
    
    public static DichotomousPairs ofDichotomousPairs( List<DichotomousPair> pairs,
                                                       Metadata meta )
    {
        return DichotomousPairs.ofDichotomousPairs( pairs, meta, null );
    }
    
    /**
     * Construct the dichotomous input from atomic {@link DichotomousPair} without any pairs for a baseline and with
     * a climatological dataset.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */
    
    public static DichotomousPairs ofDichotomousPairs( List<DichotomousPair> pairs,
                                                       Metadata meta,
                                                       VectorOfDoubles climatology )
    {
        return DichotomousPairs.ofDichotomousPairs( pairs, null, meta, null, climatology );
    }
    
    /**
     * Construct the dichotomous input from atomic {@link DichotomousPair} with pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */
    
    public static DichotomousPairs ofDichotomousPairs( List<DichotomousPair> pairs,
                                                       List<DichotomousPair> basePairs,
                                                       Metadata mainMeta,
                                                       Metadata baselineMeta,
                                                       VectorOfDoubles climatology )
    {
        DichotomousPairsBuilder b = new DichotomousPairsBuilder();
        b.addDichotomousData( pairs ).setMetadata( mainMeta ).setClimatology( climatology );
        return (DichotomousPairs) b.addDichotomousDataForBaseline( basePairs )
                                   .setMetadataForBaseline( baselineMeta )
                                   .build();
    }

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    public DichotomousPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }

        DichotomousPairs.DichotomousPairsBuilder b = new DichotomousPairs.DichotomousPairsBuilder();
        return (DichotomousPairs) b.addData( this.getRawData() )
                                   .setMetadata( this.getMetadata() )
                                   .addDataForBaseline( this.getRawDataForBaseline() )
                                   .setMetadataForBaseline( this.getMetadataForBaseline() )
                                   .setClimatology( this.getClimatology() )
                                   .build();
    }

    @Override
    public int getCategoryCount()
    {
        return 2;
    }

    /**
     * A builder to build the metric input.
     */

    public static class DichotomousPairsBuilder extends MulticategoryPairsBuilder
    {

        @Override
        public DichotomousPairs build()
        {
            return new DichotomousPairs( this );
        }

        /**
         * Convenience method for setting the input data from atomic {@link DichotomousPair}.
         * 
         * @param mainInput the main input
         * @return the builder
         */

        public MulticategoryPairsBuilder addDichotomousData( final List<DichotomousPair> mainInput )
        {
            if ( Objects.nonNull( mainInput ) )
            {
                List<MulticategoryPair> mainIn = new ArrayList<>();
                mainInput.forEach( pair -> mainIn.add( MulticategoryPair.of( new boolean[] { pair.getLeft() },
                                                                             new boolean[] { pair.getRight() } ) ) );
                addData( mainIn );
            }
            return this;
        }

        /**
         * Convenience method for setting the input data for the baseline from atomic {@link DichotomousPair}.
         * 
         * @param baselineInput the baseline input
         * @return the builder
         */

        public MulticategoryPairsBuilder addDichotomousDataForBaseline( final List<DichotomousPair> baselineInput )
        {
            if ( Objects.nonNull( baselineInput ) )
            {
                List<MulticategoryPair> baseIn = new ArrayList<>();
                baselineInput.forEach( pair -> baseIn.add( MulticategoryPair.of( new boolean[] { pair.getLeft() },
                                                                                 new boolean[] { pair.getRight() } ) ) );
                addDataForBaseline( baseIn );
            }
            return this;
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    private DichotomousPairs( final DichotomousPairsBuilder b )
    {
        super( b );
        int count = super.getCategoryCount();

        // Allow empty data or two categories
        if ( count > 0 && count != 2 )
        {
            throw new MetricInputException( "Expected one category in the dichotomous input, represented as either "
                                            + "one or two elements." );
        }
    }

}
