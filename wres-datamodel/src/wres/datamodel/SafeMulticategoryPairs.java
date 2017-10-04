package wres.datamodel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Immutable store of verification pairs associated with the outcome (true or false) of a multi-category event. The
 * categorical outcomes may be ordered or unordered. For multi-category pairs with <b>more</b> than two possible
 * outcomes, each pair should contain exactly one occurrence (true value). For efficiency, a dichotomous pair can be
 * encoded with a single indicator. The observed outcomes are recorded first, followed by the predicted outcomes.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeMulticategoryPairs implements MulticategoryPairs
{

    /**
     * The verification pairs in an immutable list.
     */

    private final List<VectorOfBooleans> mainInput;

    /**
     * Metadata associated with the verification pairs.
     */

    private final Metadata mainMeta;

    /**
     * The verification pairs for a baseline in an immutable list (may be null).
     */

    private final List<VectorOfBooleans> baselineInput;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final Metadata baselineMeta;
    
    /**
     * Climatological dataset. May be null.
     */
    
    private VectorOfDoubles climatology;    

    @Override
    public List<VectorOfBooleans> getData()
    {
        return mainInput;
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public MulticategoryPairs getBaselineData()
    {
        return DefaultDataFactory.getInstance().ofMulticategoryPairs(baselineInput, baselineMeta);
    }

    @Override
    public List<VectorOfBooleans> getDataForBaseline()
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
     * Returns the number of outcomes or categories in the dataset.
     * 
     * @return the number of categories
     */
    @Override
    public int getCategoryCount()
    {
        final int elements = mainInput.get(0).getBooleans().length;
        return elements == 2 ? 2 : elements / 2;
    }
    
    @Override
    public Iterator<VectorOfBooleans> iterator()
    {
        return mainInput.iterator();
    }      

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    static class MulticategoryPairsBuilder extends MetricInputBuilder<List<VectorOfBooleans>>
    {

        /**
         * Pairs.
         */
        private List<VectorOfBooleans> mainInput;

        /**
         * Pairs for baseline.
         */
        private List<VectorOfBooleans> baselineInput; 
        
        @Override
        public MulticategoryPairsBuilder setData(final List<VectorOfBooleans> mainInput)
        {
            this.mainInput = mainInput;
            return this;
        }

        @Override
        public MulticategoryPairsBuilder setDataForBaseline(final List<VectorOfBooleans> baselineInput)
        {
            this.baselineInput = baselineInput;
            return this;
        }

        @Override
        public SafeMulticategoryPairs build()
        {
            return new SafeMulticategoryPairs(this);
        }
    }

    /**
     * Construct the pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    SafeMulticategoryPairs(final MulticategoryPairsBuilder b)
    {
        //Ensure safe types
        DefaultDataFactory factory = (DefaultDataFactory)DefaultDataFactory.getInstance();
        mainInput = factory.safeVectorOfBooleansList(b.mainInput);
        baselineInput = Objects.nonNull(b.baselineInput) ? factory.safeVectorOfBooleansList(b.baselineInput) : null;
        mainMeta = b.mainMeta;
        baselineMeta = b.baselineMeta;
        climatology = b.climatology;

        //Validate
        if ( Objects.isNull( mainMeta ) )
        {
            throw new MetricInputException( "Specify non-null metadata for the metric input." );
        }
        if ( Objects.isNull( mainInput ) )
        {
            throw new MetricInputException( "Specify a non-null dataset for the metric input." );
        }
        if ( Objects.isNull( baselineInput ) != Objects.isNull( baselineMeta ) )
        {
            throw new MetricInputException( "Specify a non-null baseline input and associated metadata or leave both "
                                            + "null." );
        }
        if ( mainInput.contains( null ) )
        {
            throw new MetricInputException( "One or more of the pairs is null." );
        }
        if ( mainInput.isEmpty() )
        {
            throw new MetricInputException( "Cannot build the paired data with an empty input: add one or more pairs." );
        }
        if ( Objects.nonNull( baselineInput ) )
        {
            if ( baselineInput.contains( null ) )
            {
                throw new MetricInputException( "One or more of the baseline pairs is null." );
            }
            if ( baselineInput.isEmpty() )
            {
                throw new MetricInputException( "Cannot build the paired data with an empty baseline: add one or more "
                                                + "pairs." );
            }
        }
        if ( Objects.nonNull( climatology ) && climatology.size() == 0 )
        {            
            throw new MetricInputException( "Cannot build the paired data with an empty baseline: add one or more "
                                                + "pairs." );
        }        
        //Check contents
        checkEachPair(mainInput, baselineInput);
       
    }

    /**
     * Validates each pair in each input.
     * 
     * @param mainInput the main input
     * @param baselineInput the baseline input
     */

    private void checkEachPair(List<VectorOfBooleans> mainInput, List<VectorOfBooleans> baselineInput)
    {
        final List<Integer> size = new ArrayList<>();
        mainInput.forEach(t -> {
            final int count = t.size();
            if(size.isEmpty())
            {
                size.add(count);
            }
            if(!size.contains(count))
            {
                throw new MetricInputException("Two or more elements in the input have an unequal number of "
                    + "categories.");
            }
            checkPair(t);
        });
        if(Objects.nonNull(baselineInput))
        {
            baselineInput.forEach(t -> {
                final int count = t.size();
                if(size.isEmpty())
                {
                    size.add(count);
                }
                if(!size.contains(count))
                {
                    throw new MetricInputException("Two or more elements in the baseline input have an unequal number of "
                        + "categories or categories that are unequal with the main input.");
                }
                checkPair(t);
            });
        }
    }

    /**
     * Checks for exactly one observed occurrence and one predicted occurrence. Throws an exception if the condition is
     * not met.
     * 
     * @param outcomes the number of outcomes
     * @param pair the pair
     * @throws MetricInputException if the input does not contain one observed occurrence and one predicted occurrence
     */

    private void checkPair(final VectorOfBooleans pair)
    {
        final int size = pair.size();
        final int outcomes = size / 2;
        if(outcomes > 1 && size % 2 != 0)
        {
            throw new MetricInputException("The input should have an equivalent number of observed and predicted "
                + "outcomes.");
        }
        final boolean[] check = pair.getBooleans();
        if(outcomes > 1)
        {
            int o = 0;
            int p = 0;
            for(int i = 0; i < outcomes; i++)
            {
                if(check[i])
                    o++;
                if(check[i + outcomes])
                    p++;
            }
            if(o != 1 || p != 1)
            {
                throw new MetricInputException("One or more pairs do not contain exactly one observed occurrence "
                    + "and one predicted occurrence.");
            }
        }
    }

}
