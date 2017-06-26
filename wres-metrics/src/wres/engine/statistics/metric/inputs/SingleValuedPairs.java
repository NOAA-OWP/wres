package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricInputBuilder;

/**
 * Immutable store of verification pairs that comprise single-valued, continuous numerical, predictions and
 * observations. In this context, the designation "single-valued" should not be confused with "deterministic". Rather,
 * it is an input that comprises a single value. Each pair contains a single-valued observation and a corresponding
 * prediction.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class SingleValuedPairs implements MetricInput<List<PairOfDoubles>>
{

    /**
     * The verification pairs. There is one list of pairs for each variable stored in the input (e.g. including a
     * baseline).
     */

    private final List<List<PairOfDoubles>> pairs;

    /**
     * Metadata associated with the data (must be the same for all datasets).
     */

    final Metadata meta;

    @Override
    public boolean hasBaselineForSkill()
    {
        return size() == 2;
    }

    @Override
    public Metadata getMetadata()
    {
        return meta;
    }

    @Override
    public List<PairOfDoubles> getData(final int index)
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
         * Metadata.
         */
        private Metadata meta = null;

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
        public SingleValuedPairsBuilder setMetadata(final Metadata meta)
        {
            this.meta = meta;
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
        if(Objects.isNull(b.meta)) {
            throw new MetricInputException("Specify non-null metadata for the metric input.");
        }
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
        meta = b.meta;
    }
}
