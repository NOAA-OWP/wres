package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.MetricInput;

/**
 * Class for storing the verification pairs associated with the outcome (true or false) of a multi-category event. The
 * categorical outcomes may be ordered or unordered. For multi-category pairs with <b>more</b> than two possible
 * outcomes, each pair should contain exactly one occurrence (true value). For efficiency, a dichotomous pair can be
 * encoded with a single indicator. The observed outcomes are recorded first, followed by the predicted outcomes.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MulticategoryPairs implements MetricInput<List<VectorOfBooleans>>
{

    /**
     * The verification pairs.
     */

    private final List<List<VectorOfBooleans>> pairs;

    /**
     * Dimension of the data (must be the same for all datasets).
     */

    final Dimension dim;

    @Override
    public boolean hasTwo()
    {
        return size() == 2;
    }

    @Override
    public Dimension getDimension()
    {
        return null;
    }

    @Override
    public List<VectorOfBooleans> get(final int index)
    {
        return pairs.get(index);
    }

    @Override
    public int size()
    {
        return pairs.size();
    }

    /**
     * Returns the number of outcomes or categories in the dataset.
     * 
     * @return the number of categories
     */

    public int getCategoryCount()
    {
        return pairs.get(0).get(0).getBooleans().length / 2;
    }

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    public static class MulticategoryPairsBuilder implements MetricInputBuilder<List<VectorOfBooleans>>
    {

        /**
         * Pairs.
         */
        protected final List<List<VectorOfBooleans>> pairs = new ArrayList<>();

        /**
         * Dimension.
         */
        private Dimension dim = null;

        @Override
        public MulticategoryPairsBuilder add(final List<VectorOfBooleans> element)
        {
            pairs.add(element);
            return this;
        }

        @Override
        public MulticategoryPairs build()
        {
            return new MulticategoryPairs(this);
        }

        @Override
        public MulticategoryPairsBuilder setDimension(final Dimension dim)
        {
            this.dim = dim;
            return this;
        }

    }

    /**
     * Construct the single-valued pairs with a builder.
     * 
     * @param b the builder
     * @throws MetricInputException if the pairs are invalid
     */

    protected MulticategoryPairs(final MulticategoryPairsBuilder b)
    {
        //Objects.requireNonNull(b.dim,"Specify a non-null dimension for the inputs."); //TODO may require in future
        //Bounds checks
        final List<Integer> size = new ArrayList<>();
        b.pairs.stream().forEach(s -> {
            if(Objects.isNull(s) || s.isEmpty())
            {
                throw new MetricInputException("One or more of the inputs is null or empty.");
            }
            if(s.contains(null))
            {
                throw new MetricInputException("One or more of the pairs is null.");
            }
            s.stream().forEach(t -> {
                final int count = t.size();
                if(size.isEmpty())
                {
                    size.add(count);
                }
                if(!size.contains(count))
                {
                    throw new MetricInputException("The inputs have an unequal number of categories.");
                }
                final int outcomes = count / 2;
                if(outcomes > 1 && count % 2 != 0)
                {
                    throw new MetricInputException("Each multicategory input should have an equivalent number of "
                        + "observed and predicted outcomes.");
                }

                checkPair(outcomes, t);
            });

        });
        pairs = b.pairs;
        dim = b.dim;
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
