package wres.datamodel;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;

/**
 * Immutable store of verification pairs associated with a dichotomous input, i.e. a single event whose outcome is
 * recorded as occurring (true) or not occurring (false). The event is not defined as part of the input. A dichotomous
 * pair is be encoded with a single indicator.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeDichotomousPairs extends SafeMulticategoryPairs implements DichotomousPairs
{

    @Override
    public DichotomousPairs getBaselineData()
    {
        if ( !hasBaseline() )
        {
            return null;
        }
        final DataFactory metIn = DefaultDataFactory.getInstance();
        return metIn.ofDichotomousPairs( getDataForBaseline(), getMetadataForBaseline() );
    }

    @Override
    public int getCategoryCount()
    {
        return 2;
    }

    /**
     * A {@link DefaultMetricInputBuilder} to build the metric input.
     */

    static class DichotomousPairsBuilder extends MulticategoryPairsBuilder
    {

        @Override
        public SafeDichotomousPairs build()
        {
            return new SafeDichotomousPairs( this );
        }

        /**
         * Convenience method for setting the input data from atomic {@link PairOfBooleans}.
         * 
         * @param mainInput the main input
         * @return the builder
         */

        public MulticategoryPairsBuilder setDataFromAtomic( final List<PairOfBooleans> mainInput )
        {
            if ( Objects.nonNull( mainInput ) )
            {
                DataFactory d = DefaultDataFactory.getInstance();
                List<VectorOfBooleans> mainIn = new ArrayList<>();
                mainInput.forEach( pair -> mainIn.add( d.vectorOf( new boolean[] { pair.getItemOne(),
                                                                                   pair.getItemTwo() } ) ) );
                addData( mainIn );
            }
            return this;
        }

        /**
         * Convenience method for setting the input data for the baseline from atomic {@link PairOfBooleans}.
         * 
         * @param baselineInput the baseline input
         * @return the builder
         */

        public MulticategoryPairsBuilder setDataForBaselineFromAtomic( final List<PairOfBooleans> baselineInput )
        {
            if ( Objects.nonNull( baselineInput ) )
            {
                DataFactory d = DefaultDataFactory.getInstance();
                List<VectorOfBooleans> baseIn = new ArrayList<>();
                baselineInput.forEach( pair -> baseIn.add( d.vectorOf( new boolean[] { pair.getItemOne(),
                                                                                       pair.getItemTwo() } ) ) );
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

    private SafeDichotomousPairs( final DichotomousPairsBuilder b )
    {
        super( b );
        int count = super.getCategoryCount();
        // Allow empty data or two categories
        if ( count > 0 && count != 2 )
        {
            throw new MetricInputException( "Expected one outcome in the dichotomous input (two or four columns)." );
        }
    }

}
