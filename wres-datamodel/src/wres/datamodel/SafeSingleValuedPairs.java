package wres.datamodel;

import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfDoubles;

/**
 * Immutable implementation of a store of verification pairs that comprise two single-valued, continuous numerical,
 * variables.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeSingleValuedPairs implements SingleValuedPairs
{

    /**
     * The verification pairs.
     */

    private final List<PairOfDoubles> mainInput;

    /**
     * Metadata associated with the verification pairs.
     */

    private final Metadata mainMeta;

    /**
     * The verification pairs for a baseline (may be null).
     */

    private final List<PairOfDoubles> baselineInput;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final Metadata baselineMeta;
    
    /**
     * Climatological dataset. May be null.
     */
    
    private VectorOfDoubles climatology;

    @Override
    public List<PairOfDoubles> getData()
    {
        return mainInput;
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public SingleValuedPairs getBaselineData()
    {
        return DefaultDataFactory.getInstance().ofSingleValuedPairs(baselineInput, baselineMeta);
    }

    @Override
    public List<PairOfDoubles> getDataForBaseline()
    {
        return baselineInput;
    }

    @Override
    public Metadata getMetadataForBaseline()
    {
        return baselineMeta;
    }
    
    @Override
    public int size()
    {
        return mainInput.size();
    }    

    @Override
    public VectorOfDoubles getClimatology()
    {
        return climatology;
    }    
    
    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    static class SingleValuedPairsBuilder extends MetricInputBuilder<List<PairOfDoubles>>
    {

        /**
         * Pairs.
         */
        private List<PairOfDoubles> mainInput;

        /**
         * Pairs for baseline.
         */
        private List<PairOfDoubles> baselineInput;
        
        @Override
        public SingleValuedPairsBuilder setData(final List<PairOfDoubles> mainInput)
        {
            this.mainInput = mainInput;
            return this;
        }

        @Override
        public SingleValuedPairsBuilder setDataForBaseline(final List<PairOfDoubles> baselineInput)
        {
            this.baselineInput = baselineInput;
            return this;
        }

        @Override
        public SafeSingleValuedPairs build()
        {
            return new SafeSingleValuedPairs(this);
        }
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SafeSingleValuedPairs(final SingleValuedPairsBuilder b)
    {
        //Bounds checks
        if(Objects.isNull(b.mainMeta))
        {
            throw new MetricInputException("Specify non-null metadata for the metric input.");
        }
        if(Objects.isNull(b.mainInput))
        {
            throw new MetricInputException("Specify a non-null dataset for the metric input.");
        }
        if(Objects.isNull(b.baselineInput) != Objects.isNull(b.baselineMeta))
        {
            throw new MetricInputException("Specify a non-null baseline input and associated metadata or leave both null.");
        }
        if(b.mainInput.contains(null))
        {
            throw new MetricInputException("One or more of the pairs is null.");
        }
        if(!Objects.isNull(b.baselineInput) && b.baselineInput.contains(null))
        {
            throw new MetricInputException("One or more of the baseline pairs is null.");
        }
        //Ensure safe types
        DefaultDataFactory factory = (DefaultDataFactory)DefaultDataFactory.getInstance();
        mainInput = factory.safePairOfDoublesList(b.mainInput);
        baselineInput = Objects.nonNull(b.baselineInput) ? factory.safePairOfDoublesList(b.baselineInput) : null;
        mainMeta = b.mainMeta;
        baselineMeta = b.baselineMeta;
        climatology = b.climatology;
    }

}
