package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import static java.util.stream.Collectors.*;

import wres.datamodel.TupleOfDoubles;
import wres.datamodel.TuplesOfDoubles;

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

    private final List<TupleOfDoubles> pairs;

    /**
     * The baseline pairs.
     */

    private final List<TupleOfDoubles> basePairs;

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

    protected SingleValuedPairs(final TuplesOfDoubles pairs, final Dimension dim)
    {
        this(pairs, null, dim);
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

    protected SingleValuedPairs(final TuplesOfDoubles pairs2,
                                final TuplesOfDoubles basePairs,
                                final Dimension dim)
    {
        //Bounds check
        Objects.requireNonNull(pairs2, "Specify non-null input for the single-valued pairs.");
        if(pairs2.getTuplesOfDoubles().size() == 0)
        {
            throw new MetricInputException("Provide an input with one or more pairs.");
        }
        //Set the stores
        this.pairs = new ArrayList<>();
        if(basePairs != null)
        {
            //Bounds check
            if(basePairs.getTuplesOfDoubles().size() == 0)
            {
                throw new MetricInputException("Provide a baseline with one or more pairs.");
            }
            this.basePairs = new ArrayList<>();
            //Set the baseline pairs
            for(final TupleOfDoubles pair: basePairs.getTuplesOfDoubles())
            {
                if(pair.getTupleOfDoubles().length != 2)
                {
                    throw new MetricInputException("Expected single-valued pairs with only two values.");
                }
                this.basePairs.add(pair);
            }
        }
        else
        {
            this.basePairs = null;
        }
        //Set the pairs
        for(final TupleOfDoubles pair : pairs2.getTuplesOfDoubles())
        {
            if(pair.getTupleOfDoubles().length != 2)
            {
                throw new MetricInputException("Expected single-valued pairs with only two values.");
            }
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
    public List<DoubleVector> getData()
    {
        // using stream transformation for now, but if we move toward an API,
        // then we could return exactly what it is (or a copy)
        return pairs.stream()
                    .map(d -> d.getTupleOfDoubles())
                    .map(DoubleVector::new)
                    .collect(toList());
    }

    @Override
    public List<DoubleVector> getBaselineData()
    {
        // using stream transformation for now, but if we move toward an API,
        // then we could return exactly what it is (or a copy)
        return basePairs.stream()
                        .map(d -> d.getTupleOfDoubles())
                        .map(DoubleVector::new)
                        .collect(toList());
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

    private SingleValuedPairs(final List<TupleOfDoubles> pairs,
                              final List<TupleOfDoubles> basePairs,
                              final Dimension dim)
    {
        this.pairs = pairs;
        this.basePairs = basePairs;
        this.dim = dim;
    }

}
