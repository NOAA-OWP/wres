package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;

/**
 * Class for storing verification pairs that comprise single-valued, continuous numerical, predictions and observations.
 * In this context, the designation "single-valued" should not be confused with "deterministic". Rather, it is an input
 * that comprises a single value. Each pair contains a single-valued observation and a corresponding prediction.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SingleValuedPairs implements MetricInput<PairOfDoubles>
{

    /**
     * The verification pairs.
     */

    private final List<PairOfDoubles> pairs;

    /**
     * The baseline pairs.
     */

    private final List<PairOfDoubles> basePairs;

    /**
     * Dimension of the data (must be the same for all datasets).
     */

    final Dimension dim;

    /**
     * Construct the single-valued input without any pairs for a baseline. Throws an exception if the pairs are null or
     * empty or if any individual pairs do not contain two values.
     * 
     * @param pairs2 the verification pairs
     * @param dim the dimension of the input
     * @throws MetricInputException if the pairs are invalid
     */

    protected SingleValuedPairs(final List<PairOfDoubles> pairs2, final Dimension dim)
    {
        this(pairs2, null, dim);
    }

    /**
     * Construct the single-valued input with a baseline. Throws an exception if the pairs are null or empty or if the
     * baseline pairs are empty or if any individual pairs do not contain two values. The baseline pairs may be null.
     * 
     * @param pairs2 the single-valued pairs
     * @param basePairs the baseline pairs
     * @param dim the dimension of the input
     * @throws MetricInputException if the pairs are invalid
     */

    protected SingleValuedPairs(final List<PairOfDoubles> pairs2,
                                final List<PairOfDoubles> basePairs,
                                final Dimension dim)
    {
        //Bounds check
        Objects.requireNonNull(pairs2, "Specify non-null input for the single-valued pairs.");
        if(pairs2.size() == 0)
        {
            throw new MetricInputException("Provide an input with one or more pairs.");
        }
        //Set the stores
        this.pairs = new ArrayList<>();
        if(basePairs != null)
        {
            //Bounds check
            if(basePairs.size() == 0)
            {
                throw new MetricInputException("Provide a baseline with one or more pairs.");
            }
            this.basePairs = new ArrayList<>();
            //Set the baseline pairs
            for(final PairOfDoubles pair: basePairs)
            {
                this.basePairs.add(pair);
            }
        }
        else
        {
            this.basePairs = null;
        }
        //Set the pairs
        for(final PairOfDoubles pair : pairs2)
        {
            this.pairs.add(pair);
        }
        this.dim = dim;
    }

    @Override
    public boolean hasBaseline()
    {
        return basePairs != null;
    }

    @Override
    public Dimension getDimension()
    {
        return dim;
    }

    @Override
    public SingleValuedPairs getBaseline()
    {
        SingleValuedPairs returnMe = null;
        if(hasBaseline())
        {
            returnMe = new SingleValuedPairs(basePairs, null, dim);
        }
        return returnMe;
    }

    @Override
    public List<PairOfDoubles> getData()
    {
        return pairs;
    }

    @Override
    public List<PairOfDoubles> getBaselineData()
    {
        return basePairs;
    }

    @Override
    public int size()
    {
        return pairs.size();
    }

    @Override
    public int baseSize()
    {
        return basePairs.size();
    }
}
