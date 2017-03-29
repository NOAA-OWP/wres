package wres.engine.statistics.metric.inputs;

import java.util.Objects;

/**
 * Class for storing the verification pairs associated with a dichotomous input, i.e. a single event whose outcome is
 * recorded as occurring (true) or not occurring (false). The event is not defined as part of the input. A dichotomous
 * pair is be encoded with a single indicator.
 * 
 * @author james.brown@hydrosolved.com
 */
public class DichotomousPairs extends MulticategoryPairs
{

    /**
     * Construct the dichotomous input without any pairs for a baseline. Throws an exception if either input is null or
     * empty.
     * 
     * @param pairs the dichotomous pairs
     * @throws MetricInputException if either input is null or empty.
     */

    public DichotomousPairs(final boolean[][] pairs)
    {
        this(pairs, null);
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

    public DichotomousPairs(final boolean[][] pairs, final boolean[][] basePairs)
    {
        super(checkPairs(pairs), basePairs);
    }

    @Override
    public int getCategoryCount()
    {
        return 2;
    }

    /**
     * Throws an exception if the input does not contain two columns.
     * 
     * @throws MetricInputException if the input does not contain two columns
     */

    private static boolean[][] checkPairs(final boolean[][] pairs)
    {
        Objects.requireNonNull(pairs, "Specify non-null input for the dichotomous pairs.");
        if(pairs[0].length != 2)
        {
            throw new MetricInputException("Expected two columns in the input.");
        }
        return pairs;
    }
}
