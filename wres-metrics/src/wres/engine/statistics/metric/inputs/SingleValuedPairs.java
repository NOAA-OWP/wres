package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.MetricInput;

/**
 * Immutable store of verification pairs that comprise single-valued, continuous numerical, predictions and
 * observations. In this context, the designation "single-valued" should not be confused with "deterministic". Rather,
 * it is an input that comprises a single value. Each pair contains a single-valued observation and a corresponding
 * prediction.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SingleValuedPairs implements MetricInput<List<PairOfDoubles>>
{

    /**
     * The verification pairs. There is one list of pairs for each variable stored in the input (e.g. including a
     * baseline).
     */

    private final List<List<PairOfDoubles>> pairs;

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
        return dim;
    }

    @Override
    public List<PairOfDoubles> get(final int index)
    {
        return Collections.unmodifiableList(pairs.get(index));
    }

    @Override
    public int size()
    {
        return pairs.size();
    }

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    public static class SingleValuedPairsBuilder implements MetricInputBuilder<List<PairOfDoubles>>
    {

        /**
         * Pairs.
         */
        private final List<List<PairOfDoubles>> pairs = new ArrayList<>();

        /**
         * Dimension.
         */
        private Dimension dim = null;

        @Override
        public SingleValuedPairsBuilder add(final List<PairOfDoubles> element)
        {
            pairs.add(element);
            return this;
        }

        @Override
        public SingleValuedPairs build()
        {
            return new SingleValuedPairs(this);
        }

        @Override
        public SingleValuedPairsBuilder setDimension(final Dimension dim)
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

    protected SingleValuedPairs(final SingleValuedPairsBuilder b)
    {
        //Objects.requireNonNull(b.dim,"Specify a non-null dimension for the inputs."); //TODO may require in future
        //Bounds checks
        b.pairs.stream().forEach(s -> {
            if(Objects.isNull(s))
            {
                throw new MetricInputException("One or more of the inputs is null.");
            }
            if(s.contains(null))
            {
                throw new MetricInputException("One or more of the pairs is null.");
            }
        });
        pairs = b.pairs;
        dim = b.dim;
    }
}
