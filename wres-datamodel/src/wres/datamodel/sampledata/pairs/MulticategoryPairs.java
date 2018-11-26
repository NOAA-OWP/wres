package wres.datamodel.sampledata.pairs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;

/**
 * Immutable store of verification pairs associated with the outcome (true or false) of a multi-category event.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MulticategoryPairs extends Pairs<MulticategoryPair>
{

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws SampleDataException if the inputs are invalid
     */

    public static MulticategoryPairs ofMulticategoryPairs( List<MulticategoryPair> pairs, SampleMetadata meta )
    {
        return MulticategoryPairs.ofMulticategoryPairs( pairs, null, meta, null, null );
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

    public static MulticategoryPairs ofMulticategoryPairs( MulticategoryPairs pairs,
                                                           SampleMetadata overrideMainMeta,
                                                           SampleMetadata overrideBaselineMeta )
    {
        return MulticategoryPairs.ofMulticategoryPairs( pairs.getRawData(),
                                                        pairs.getRawDataForBaseline(),
                                                        overrideMainMeta,
                                                        overrideBaselineMeta,
                                                        pairs.getClimatology() );
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws SampleDataException if the inputs are invalid
     */

    public static MulticategoryPairs ofMulticategoryPairs( List<MulticategoryPair> pairs,
                                                           List<MulticategoryPair> basePairs,
                                                           SampleMetadata mainMeta,
                                                           SampleMetadata baselineMeta,
                                                           VectorOfDoubles climatology )
    {
        MulticategoryPairsBuilder b =
                new MulticategoryPairsBuilder();

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
    public MulticategoryPairs getBaselineData()
    {
        if ( Objects.isNull( this.getRawDataForBaseline() ) )
        {
            return null;
        }
        return MulticategoryPairs.ofMulticategoryPairs( this.getRawDataForBaseline(), this.getMetadataForBaseline() );
    }

    /**
     * Returns the number of outcomes or categories in the dataset.
     * 
     * @return the number of categories
     */

    public int getCategoryCount()
    {
        if ( this.getRawData().isEmpty() )
        {
            return 0;
        }
        // One element allowed as shorthand for two categories       
        final int elements = this.getRawData().get( 0 ).getLeft().length;

        return elements == 1 ? 2 : elements;
    }

    /**
     * A builder to build the metric input.
     */

    public static class MulticategoryPairsBuilder extends BasicPairsBuilder<MulticategoryPair>
    {
        @Override
        public MulticategoryPairs build()
        {
            return new MulticategoryPairs( this );
        }
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws SampleDataException if the pairs are invalid
     */

    MulticategoryPairs( final MulticategoryPairsBuilder b )
    {
        super( b );

        //Check contents
        checkEachPair( this.getRawData(), this.getRawDataForBaseline() );

    }

    /**
     * Validates each pair in each input.
     * 
     * @param mainInput the main input
     * @param baselineInput the baseline input
     */

    private void checkEachPair( List<MulticategoryPair> mainInput, List<MulticategoryPair> baselineInput )
    {
        final List<Integer> size = new ArrayList<>();
        mainInput.forEach( t -> {
            final int count = t.getLeft().length;
            if ( size.isEmpty() )
            {
                size.add( count );
            }
            if ( !size.contains( count ) )
            {
                throw new SampleDataException( "Two or more elements in the input have an unequal number of "
                                                + "categories." );
            }
        } );
        if ( Objects.nonNull( baselineInput ) )
        {
            baselineInput.forEach( t -> {
                final int count = t.getLeft().length;
                if ( size.isEmpty() )
                {
                    size.add( count );
                }
                if ( !size.contains( count ) )
                {
                    throw new SampleDataException( "Two or more elements in the baseline input have an unequal number of "
                                                    + "categories or categories that are unequal with the main input." );
                }
            } );
        }
    }

}
