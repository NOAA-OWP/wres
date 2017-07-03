package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfBooleans;

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
     * The verification pairs.
     */

    private final List<VectorOfBooleans> mainInput;

    /**
     * Metadata associated with the verification pairs.
     */

    private final Metadata mainMeta;

    /**
     * The verification pairs for a baseline (may be null).
     */

    private final List<VectorOfBooleans> baselineInput;

    /**
     * Metadata associated with the baseline verification pairs (may be null).
     */

    private final Metadata baselineMeta;

    @Override
    public boolean hasBaseline()
    {
        return !Objects.isNull(baselineInput);
    }

    @Override
    public List<VectorOfBooleans> getData()
    {
        return Collections.unmodifiableList(mainInput);
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public MulticategoryPairs getBaselineData()
    {
        final MetricInputFactory metIn = DefaultMetricInputFactory.of();
        return metIn.ofMulticategoryPairs(baselineInput, baselineMeta);
    }          
    
    @Override
    public List<VectorOfBooleans> getDataForBaseline()
    {
        return Collections.unmodifiableList(baselineInput);
    }

    @Override
    public Metadata getMetadataForBaseline()
    {
        return baselineMeta;
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

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    protected static class MulticategoryPairsBuilder implements MetricInputBuilder<List<VectorOfBooleans>>
    {

        /**
         * Pairs.
         */
        private List<VectorOfBooleans> mainInput;

        /**
         * Pairs for baseline.
         */
        private List<VectorOfBooleans> baselineInput;

        /**
         * Metadata for input.
         */

        private Metadata mainMeta;

        /**
         * Metadata for baseline.
         */

        private Metadata baselineMeta;

        @Override
        public MulticategoryPairsBuilder setData(final List<VectorOfBooleans> mainInput)
        {
            this.mainInput = mainInput;
            return this;
        }

        @Override
        public MulticategoryPairsBuilder setMetadata(final Metadata mainMeta)
        {
            this.mainMeta = mainMeta;
            return this;
        }

        @Override
        public MulticategoryPairsBuilder setDataForBaseline(final List<VectorOfBooleans> baselineInput)
        {
            this.baselineInput = baselineInput;
            return this;
        }

        @Override
        public MulticategoryPairsBuilder setMetadataForBaseline(final Metadata baselineMeta)
        {
            this.baselineMeta = baselineMeta;
            return this;
        }

        @Override
        public MulticategoryPairs build()
        {
            return new SafeMulticategoryPairs(this);
        }
    }

    /**
     * Construct the single-valued pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    protected SafeMulticategoryPairs(final MulticategoryPairsBuilder b)
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
            throw new MetricInputException("Specify a non-null baseline input and associated metadata or leave both "
                + "null.");
        }
        final List<Integer> size = new ArrayList<>();
        b.mainInput.stream().forEach(t -> {
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
            final int outcomes = count / 2;
            if(outcomes > 1 && count % 2 != 0)
            {
                throw new MetricInputException("The input should have an equivalent number of observed and predicted "
                    + "outcomes.");
            }

            checkPair(outcomes, t);
        });
        if(!Objects.isNull(b.baselineInput))
        {
            b.baselineInput.stream().forEach(t -> {
                final int count = t.size();
                if(size.isEmpty())
                {
                    size.add(count);
                }
                if(!size.contains(count))
                {
                    throw new MetricInputException("Two or more elements in the baseline input have an unequal number of "
                        + "categories.");
                }
                final int outcomes = count / 2;
                if(outcomes > 1 && count % 2 != 0)
                {
                    throw new MetricInputException("The baseline input should have an equivalent number of observed and "
                        + "predicted outcomes.");
                }
                checkPair(outcomes, t);
            });
        }
        //Set
        mainInput = b.mainInput;
        mainMeta = b.mainMeta;
        baselineInput = b.baselineInput;
        baselineMeta = b.baselineMeta;
    }

    /**
     * Checks for exactly one observed occurrence and one predicted occurrence. Throws an exception if the condition is
     * not met.
     * 
     * @param outcomes the number of outcomes
     * @param pair the pair
     * @throws MetricInputException if the input does not contain one observed occurrence and one predicted occurrence
     */

    private void checkPair(final int outcomes, final VectorOfBooleans pair)
    {

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
