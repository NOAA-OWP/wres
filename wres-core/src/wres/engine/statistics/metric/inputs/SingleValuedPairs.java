package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.Objects;

import gov.noaa.wres.datamodel.Dimension;
import gov.noaa.wres.datamodel.MetricInput;

/**
 * Class for storing verification pairs that comprise single-valued, continuous numerical, predictions and observations.
 * In this context, the designation "single-valued" should not be confused with "deterministic". Rather, it is an input
 * that comprises a single value. Each pair contains a single-valued observation and a corresponding prediction.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SingleValuedPairs implements MetricInput<DoubleVector>
{

    /**
     * The verification pairs.
     */

    final ArrayList<DoubleVector> pairs;

    /**
     * The baseline pairs.
     */

    final ArrayList<DoubleVector> basePairs;

    /**
     * Dimension of the data (must be the same for all datasets).
     */

    final Dimension dim;

    /**
     * Construct the single-valued input without any pairs for a baseline. Throws an exception if the pairs are null or
     * empty or if any individual pairs do not contain two values.
     * 
     * @param pairs the verification pairs
     * @param dim the dimension of the input
     * @throws MetricInputException if the pairs are invalid
     */

    protected SingleValuedPairs(final double[][] pairs, final Dimension dim)
    {
        this(pairs, null, dim);
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

    protected SingleValuedPairs(final double[][] pairs, final double[][] basePairs, final Dimension dim)
    {
        //Bounds check
        Objects.requireNonNull(pairs, "Specify non-null input for the single-valued pairs.");
        if(pairs.length == 0)
        {
            throw new MetricInputException("Provide an input with one or more pairs.");
        }
        //Set the stores
        this.pairs = new ArrayList<>();
        if(basePairs != null)
        {
            //Bounds check
            if(basePairs.length == 0)
            {
                throw new MetricInputException("Provide a baseline with one or more pairs.");
            }
            this.basePairs = new ArrayList<>();
            //Set the baseline pairs
            for(final double[] pair: basePairs)
            {
                if(pair.length != 2)
                {
                    throw new MetricInputException("Expected single-valued pairs with only two values.");
                }
                this.basePairs.add(PairFactory.getDoublePair(pair));
            }
        }
        else
        {
            this.basePairs = null;
        }
        //Set the pairs
        for(final double[] pair: pairs)
        {
            if(pair.length != 2)
            {
                throw new MetricInputException("Expected single-valued pairs with only two values.");
            }
            this.pairs.add(PairFactory.getDoublePair(pair));
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
    public ArrayList<DoubleVector> getData()
    {
        return pairs;
    }

    @Override
    public ArrayList<DoubleVector> getBaselineData()
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

    /**
     * Construct the single-valued input with a baseline. Throws an exception if the pairs are null or empty or if the
     * baseline pairs are empty. The baseline pairs may be null.
     * 
     * @param pairs the single-valued pairs
     * @param basePairs the baseline pairs
     * @param dim the dimension of the input
     * @throws MetricInputException
     */

    private SingleValuedPairs(final ArrayList<DoubleVector> pairs,
                              final ArrayList<DoubleVector> basePairs,
                              final Dimension dim)
    {
        this.pairs = pairs;
        this.basePairs = basePairs;
        this.dim = dim;
    }

}
