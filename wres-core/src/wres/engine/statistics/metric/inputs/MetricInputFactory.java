package wres.engine.statistics.metric.inputs;

/**
 * A factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricInputFactory
{

    /**
     * Construct the dichotomous input without any pairs for a baseline. Throws an exception if either input is null or
     * empty.
     * 
     * @param pairs the dichotomous pairs
     * @throws MetricInputException if either input is null or empty.
     */

    public static DichotomousPairs ofDichotomousPairs(final boolean[][] pairs)
    {
        return ofDichotomousPairs(pairs, null);
    }

    /**
     * Construct the dichotomous input with a baseline. The baseline may be null. The pairs have two columns, with the
     * observed outcome in the first column and the predicted outcome in the second column. If the baseline pairs are
     * non-null, they must have two columns and cannot be empty. Throws an exception if the input is null or empty or if
     * the baseline is empty or if either inputs do not contain two columns.
     * 
     * @param pairs the dichotomous pairs
     * @param basePairs the dichotomous pairs for the baseline
     * @throws MetricInputException if the input is null or empty or if the baseline is empty or if either inputs do not
     *             contain two columns.
     */

    public static DichotomousPairs ofDichotomousPairs(final boolean[][] pairs, final boolean[][] basePairs)
    {
        return new DichotomousPairs(pairs, basePairs);
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline. Throws an exception if the pairs are
     * null, empty, incomplete, or out of bounds.
     * 
     * @param pairs the discrete probability pairs
     * @throws MetricInputException if the pairs are null, empty, incomplete, or out of bounds
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final double[][] pairs)
    {
        return ofDiscreteProbabilityPairs(pairs, null);
    }

    /**
     * Construct the probability input with a baseline. Throws an exception if the pairs are null, empty, incomplete or
     * out of bounds or if the baseline is empty, incomplete or out of bounds. The baseline may be null.
     * 
     * @param pairs the discrete probability pairs
     * @param basePairs the baseline pairs
     * @throws MetricInputException if the pairs are null, empty, incomplete, or out of bounds
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final double[][] pairs,
                                                                      final double[][] basePairs)
    {
        return new DiscreteProbabilityPairs(pairs, basePairs);
    }

    /**
     * Construct the multicategory input without any pairs for a baseline. The pairs have twice as many columns as
     * possible outcomes, with the observed outcomes in the first columns and the predicted outcomes in the last
     * columns. Throws an exception if the input is null or empty or contains an odd number of columns.
     * 
     * @param pairs the multicategory pairs
     * @throws MetricInputException if the input is null or empty or contains an odd number of columns
     */

    public static MulticategoryPairs ofMulticategoryPairs(final boolean[][] pairs)
    {
        return ofMulticategoryPairs(pairs, null);
    }

    /**
     * Construct the multicategory input with a baseline. The baseline may be null. The pairs have twice as many columns
     * as possible outcomes, with the observed outcomes in the first columns and the predicted outcomes in the last
     * columns. If the baseline pairs are non-null, they should have as many columns as the main pairs. Unless the pair
     * refers to a dichotomous event (two possible outcomes), each pair should have exactly one observed occurrence and
     * one predicted occurrence, denoted by a single true entry for each of the observed and predicted outcomes. Throws
     * an exception if the input is null or empty or contains an odd number of columns or a the pair does not contain
     * one observed occurrence and one predicted occurrence (for events with more than two possible outcomes).
     * 
     * @param pairs the multicategory pairs
     * @param basePairs the multicategory baseline pairs
     * @throws MetricInputException if the inputs are unexpected
     */

    public static MulticategoryPairs ofMulticategoryPairs(final boolean[][] pairs, final boolean[][] basePairs)
    {
        return new MulticategoryPairs(pairs, basePairs);
    }

    /**
     * Construct the single-valued input without any pairs for a baseline. Throws an exception if the pairs are null or
     * empty or if any individual pairs do not contain two values.
     * 
     * @param pairs the verification pairs
     * @param dim the dimension of the input
     * @throws MetricInputException if the pairs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs(final double[][] pairs, final Dimension dim)
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
     * @throws MetricInputException if the pairs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs(final double[][] pairs,
                                                        final double[][] basePairs,
                                                        final Dimension dim)
    {
        return new SingleValuedPairs(pairs, basePairs, null);
    }

    /**
     * Prevent construction.
     */

    private MetricInputFactory()
    {

    }

}
