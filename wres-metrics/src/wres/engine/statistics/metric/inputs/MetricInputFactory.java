package wres.engine.statistics.metric.inputs;

import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.Dimension;

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
     * Construct the dichotomous input without any pairs for a baseline. Throws an exception if the pairs are null or
     * empty or if any individual pairs do not contain two values.
     * 
     * @param pairs the verification pairs
     * @return the pairs
     * @throws MetricInputException if the pairs are invalid
     */

    public static DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs)
    {
        return (DichotomousPairs)new DichotomousPairs.DichotomousPairsBuilder().add(pairs).build();
    }

    /**
     * Construct the multicategory input without any pairs for a baseline. Throws an exception if the pairs are null or
     * empty or if any individual pairs do not contain two values.
     * 
     * @param pairs the verification pairs
     * @return the pairs
     * @throws MetricInputException if the pairs are invalid
     */

    public static MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs)
    {
        return new MulticategoryPairs.MulticategoryPairsBuilder().add(pairs).build();
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline. Throws an exception if the pairs are
     * null, empty, incomplete, or out of bounds.
     * 
     * @param pairs the discrete probability pairs
     * @throws MetricInputException if the pairs are null, empty, incomplete, or out of bounds
     * @return the pairs
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs)
    {
        return ofDiscreteProbabilityPairs(pairs, null);
    }

    /**
     * Construct the discrete probability input with a baseline. Throws an exception if the pairs are null, empty,
     * incomplete or out of bounds or if the baseline is empty, incomplete or out of bounds. The baseline may be null.
     * 
     * @param pairs the discrete probability pairs
     * @param basePairs the baseline pairs
     * @throws MetricInputException if the pairs are null, empty, incomplete, or out of bounds
     * @return the pairs
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final List<PairOfDoubles> basePairs)
    {
        final DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder b =
                                                                         new DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder();
        b.add(pairs);
        if(!Objects.isNull(basePairs))
        {
            b.add(basePairs);
        }
        return b.build();
    }

    /**
     * Construct the single-valued input without any pairs for a baseline. Throws an exception if the pairs are null or
     * empty or if any individual pairs do not contain two values.
     * 
     * @param pairs the verification pairs
     * @param dim the dimension of the input
     * @return the pairs
     * @throws MetricInputException if the pairs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Dimension dim)
    {
        return ofSingleValuedPairs(pairs, null, dim);
    }

    /**
     * Construct the single-valued input with a baseline. Throws an exception if the pairs are null or empty or if the
     * baseline pairs are empty or if any individual pairs do not contain two values. The baseline pairs may be null.
     * 
     * @param pairs the single-valued pairs
     * @param basePairs the baseline pairs
     * @param dim the dimension of the input
     * @return the pairs
     * @throws MetricInputException if the pairs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                        final List<PairOfDoubles> basePairs,
                                                        final Dimension dim)
    {
        final SingleValuedPairs.SingleValuedPairsBuilder b = new SingleValuedPairs.SingleValuedPairsBuilder();
        b.setDimension(dim);
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
     * @param dim the dimension
     * @param <T> the output pairs
     * @return the casted pairs
     */

    @SuppressWarnings("unchecked")
    public static <T extends SingleValuedPairs> T ofExtendsSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                                             final Dimension dim)
    {
        return (T)ofSingleValuedPairs(pairs, null, dim);
    }

    /**
     * Prevent construction.
     */

    private MetricInputFactory()
    {

    }

}
