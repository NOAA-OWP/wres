package wres.datamodel.metric;

import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.VectorOfDoubles;

/**
 * Immutable implementation of a store of verification pairs that comprise a single value and an ensemble of values.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeEnsemblePairs implements EnsemblePairs
{

    /**
     * The verification pairs.
     */

    private final List<PairOfDoubleAndVectorOfDoubles> mainInput;

    /**
     * Metadata associated with the verification pairs.
     */

    private final Metadata mainMeta;

    /**
     * The verification pairs for a baseline (may be null).
     */

    private final List<PairOfDoubleAndVectorOfDoubles> baselineInput;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final Metadata baselineMeta;
    
    /**
     * Climatological dataset. May be null.
     */
    
    private VectorOfDoubles climatology;    

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> getData()
    {
        return mainInput;
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public EnsemblePairs getBaselineData()
    {
        return DefaultDataFactory.getInstance().ofEnsemblePairs(baselineInput, baselineMeta);
    }

    @Override
    public List<PairOfDoubleAndVectorOfDoubles> getDataForBaseline()
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

    static class EnsemblePairsBuilder extends MetricInputBuilder<List<PairOfDoubleAndVectorOfDoubles>>
    {

        /**
         * Pairs.
         */
        private List<PairOfDoubleAndVectorOfDoubles> mainInput;

        /**
         * Pairs for baseline.
         */
        private List<PairOfDoubleAndVectorOfDoubles> baselineInput;

        @Override
        public EnsemblePairsBuilder setData(final List<PairOfDoubleAndVectorOfDoubles> mainInput)
        {
            this.mainInput = mainInput;
            return this;
        }
        
        @Override
        public EnsemblePairsBuilder setDataForBaseline(final List<PairOfDoubleAndVectorOfDoubles> baselineInput)
        {
            this.baselineInput = baselineInput;
            return this;
        }   

        @Override
        public SafeEnsemblePairs build()
        {
            return new SafeEnsemblePairs(this);
        }

    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SafeEnsemblePairs(final EnsemblePairsBuilder b)
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
        mainInput = factory.safePairOfDoubleAndVectorOfDoublesList(b.mainInput);
        baselineInput =
                      Objects.nonNull(b.baselineInput) ? factory.safePairOfDoubleAndVectorOfDoublesList(b.baselineInput) : null;
        mainMeta = b.mainMeta;
        baselineMeta = b.baselineMeta;
        climatology = b.climatology;
    }

}
