package wres.engine.statistics.metric.inputs;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import wres.datamodel.DataFactory;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.MetricInput;

/**
 * Class for storing the verification pairs associated with the outcome (true or false) of a multi-category event. The
 * categorical outcomes may be ordered or unordered. For multi-category pairs with <b>more</b> than two possible
 * outcomes, each pair should contain exactly one occurrence (true value). For efficiency, a dichotomous pair can be
 * encoded with a single indicator.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MulticategoryPairs implements MetricInput<VectorOfBooleans>
{

    /**
     * The multicategory pairs.
     */

    final List<VectorOfBooleans> pairs;

    /**
     * The multicategory pairs for the baseline.
     */

    final List<VectorOfBooleans> basePairs;

    /**
     * Construct the multicategory input without any pairs for a baseline. The pairs have twice as many columns as
     * possible outcomes, with the observed outcomes in the first columns and the predicted outcomes in the last
     * columns. Throws an exception if the input is null or empty or contains an odd number of columns.
     * 
     * @param pairs the multicategory pairs
     * @throws MetricInputException if the input is null or empty or contains an odd number of columns
     */

    protected MulticategoryPairs(final boolean[][] pairs)
    {
        this(pairs, null);
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

    protected MulticategoryPairs(final boolean[][] pairs, final boolean[][] basePairs)
    {
        //Bounds check
        Objects.requireNonNull(pairs, "Specify non-null input for the multicategory pairs.");
        if(pairs.length == 0)
        {
            throw new MetricInputException("Provide an input with one or more pairs.");
        }
        //Set the stores
        this.pairs = new ArrayList<>();
        this.basePairs = new ArrayList<>();
        final int outcomes = pairs[0].length / 2;
        final DataFactory dataFactory = wres.datamodel.impl.DataFactory.instance();

        if(outcomes > 1 && outcomes % 2 != 0)
        {
            throw new MetricInputException("Expected a multicategory input with an equivalent number of observed and "
                + "predicted outcomes.");
        }
        if(basePairs != null)
        {
            //Bounds check
            if(basePairs.length == 0)
            {
                throw new MetricInputException("Provide paired inputs for the baseline that contain one or more elements.");
            }
            //Set the baseline pairs
            for(final boolean[] pair: basePairs)
            {
                if(pair.length / 2 != outcomes)
                {
                    throw new MetricInputException("Expected a multicategory baseline with " + outcomes + " outcomes.");
                }
                checkPair(outcomes, pair);
                this.pairs.add(dataFactory.vectorOf(pair));
            }
        }
        //Set the pairs
        for(final boolean[] pair: pairs)
        {
            if(pair.length / 2 != outcomes)
            {
                throw new MetricInputException("Expected multicategory pairs with with " + outcomes + " outcomes.");
            }
            checkPair(outcomes, pair);
            this.pairs.add(dataFactory.vectorOf(pair));
        }
    }

    @Override
    public boolean hasBaseline()
    {
        return basePairs != null;
    }

    @Override
    public Dimension getDimension()
    {
        return null;
    }

    @Override
    public List<VectorOfBooleans> getData()
    {
        return pairs;
    }

    @Override
    public List<VectorOfBooleans> getBaselineData()
    {
        return basePairs;
    }

    @Override
    public MulticategoryPairs getBaseline()
    {
        MulticategoryPairs returnMe = null;
        if(hasBaseline())
        {
            returnMe = new MulticategoryPairs(basePairs, null);
        }
        return returnMe;
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
     * Returns the number of outcomes or categories in the dataset.
     * 
     * @return the number of categories
     */

    public int getCategoryCount()
    {
        return pairs.get(0).getBooleans().length / 2;
    }

    /**
     * Construct the multicategory input with a baseline. Throws an exception if the pairs are null or empty or if the
     * baseline pairs are empty. The baseline pairs may be null.
     * 
     * @param pairs the single-valued pairs
     * @param basePairs the baseline pairs
     * @param dim the dimension of the input
     * @throws MetricInputException
     */

    private MulticategoryPairs(final List<VectorOfBooleans> pairs, final List<VectorOfBooleans> basePairs)
    {
        this.pairs = pairs;
        this.basePairs = basePairs;
    }

    /**
     * Checks for exactly one observed occurrence and one predicted occurrence. Throws an exception if the condition is
     * not met.
     * 
     * @param outcomes the number of outcomes
     * @param pair the pair
     * @throws MetricInputException if the input does not contain one observed occurrence and one predicted occurrence
     */

    private void checkPair(final int outcomes, final boolean[] pair)
    {
        if(outcomes > 1)
        {
            int o = 0;
            int p = 0;
            for(int i = 0; i < outcomes; i++)
            {
                if(pair[i])
                    o++;
                if(pair[i + outcomes])
                    p++;
            }
            if(o != 1 || p != 1)
            {
                throw new MetricInputException("One or more pairs do not contain exactly one observed occurrence "
                    + "and one predicted occurrence.");
            }
        }
    }

}
