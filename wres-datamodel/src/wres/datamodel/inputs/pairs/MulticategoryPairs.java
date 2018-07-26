package wres.datamodel.inputs.pairs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;

/**
 * Immutable store of verification pairs associated with the outcome (true or false) of a multi-category event.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MulticategoryPairs extends BasicPairs<MulticategoryPair>
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */
    @Override
    public MulticategoryPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        return DataFactory.ofMulticategoryPairs( this.getRawDataForBaseline(), this.getMetadataForBaseline() );
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
     * @throws MetricInputException if the pairs are invalid
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
                throw new MetricInputException( "Two or more elements in the input have an unequal number of "
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
                    throw new MetricInputException( "Two or more elements in the baseline input have an unequal number of "
                                                    + "categories or categories that are unequal with the main input." );
                }
            } );
        }
    }

}
