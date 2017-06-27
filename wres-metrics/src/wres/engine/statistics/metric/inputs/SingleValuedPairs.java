package wres.engine.statistics.metric.inputs;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.Metadata;
import wres.datamodel.metric.MetricInput;

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

    @Override
    public boolean hasBaseline()
    {
        return !Objects.isNull(baselineInput);
    }

    @Override
    public List<PairOfDoubles> getData()
    {
        return Collections.unmodifiableList(mainInput);
    }

    @Override
    public Metadata getMetadata()
    {
        return mainMeta;
    }

    @Override
    public List<PairOfDoubles> getDataForBaseline()
    {
        return Collections.unmodifiableList(baselineInput);
    }

    @Override
    public Metadata getMetadataForBaseline()
    {
        return baselineMeta;
    }

    /**
     * A {@link MetricInputBuilder} to build the metric input.
     */

    public static class SingleValuedPairsBuilder implements MetricInputBuilder<List<PairOfDoubles>>
    {

        /**
         * Pairs.
         */
        private List<PairOfDoubles> mainInput;

        /**
         * Pairs for baseline.
         */
        private List<PairOfDoubles> baselineInput;

        /**
         * Metadata for input.
         */

        private Metadata mainMeta;

        /**
         * Metadata for baseline.
         */

        private Metadata baselineMeta;

        @Override
        public SingleValuedPairsBuilder setData(final List<PairOfDoubles> mainInput)
        {
            this.mainInput = mainInput;
            return this;
        }

        @Override
        public SingleValuedPairsBuilder setMetadata(final Metadata mainMeta)
        {
            this.mainMeta = mainMeta;
            return this;
        }

        @Override
        public SingleValuedPairsBuilder setDataForBaseline(final List<PairOfDoubles> baselineInput)
        {
            this.baselineInput = baselineInput;
            return this;
        }

        @Override
        public SingleValuedPairsBuilder setMetadataForBaseline(final Metadata baselineMeta)
        {
            this.baselineMeta = baselineMeta;
            return this;
        }

        @Override
        public SingleValuedPairs build()
        {
            return new SingleValuedPairs(this);
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
        if(b.mainInput.contains(null)) {
            throw new MetricInputException("One or more of the pairs is null.");
        }
        if(!Objects.isNull(b.baselineInput) && b.baselineInput.contains(null)) {
            throw new MetricInputException("One or more of the baseline pairs is null.");
        }      
        mainInput = b.mainInput;
        mainMeta = b.mainMeta;
        baselineInput = b.baselineInput;
        baselineMeta = b.baselineMeta;
    }
}
