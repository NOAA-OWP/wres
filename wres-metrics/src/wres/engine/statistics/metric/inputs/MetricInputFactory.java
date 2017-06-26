package wres.engine.statistics.metric.inputs;

import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.Metadata;

/**
 * A factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricInputFactory
{

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return (DichotomousPairs)new DichotomousPairs.DichotomousPairsBuilder().add(pairs).setMetadata(meta).build();
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return new MulticategoryPairs.MulticategoryPairsBuilder().add(pairs).setMetadata(meta).build();
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofDiscreteProbabilityPairs(pairs, null, meta);
    }

    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param basePairs the baseline pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final List<PairOfDoubles> basePairs,
                                                                      final Metadata meta)
    {
        final DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder b =
                                                                         new DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder();
        b.add(pairs);
        b.setMetadata(meta);
        if(!Objects.isNull(basePairs))
        {
            b.add(basePairs);
        }
        return b.build();
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofSingleValuedPairs(pairs, null, meta);
    }

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param basePairs the baseline pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                        final List<PairOfDoubles> basePairs,
                                                        final Metadata meta)
    {
        final SingleValuedPairs.SingleValuedPairsBuilder b = new SingleValuedPairs.SingleValuedPairsBuilder();
        b.setMetadata(meta);
        b.add(pairs);
        if(!Objects.isNull(basePairs))
        {
            b.add(basePairs);
        }
        return b.build();
    }

    /**
     * Return a cast of the inputs pairs.
     * 
     * @param pairs the input pairs
     * @param meta the dimension
     * @param <T> the output pairs
     * @return the casted pairs
     */

    @SuppressWarnings("unchecked")
    public static <T extends SingleValuedPairs> T ofExtendsSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                                             final Metadata meta)
    {
        return (T)ofSingleValuedPairs(pairs, null, meta);
    }

    /**
     * Prevent construction.
     */

    private MetricInputFactory()
    {

    }

}
