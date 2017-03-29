package wres.engine.statistics.metric.inputs;

/**
 * Factory class for constructing verification pairs from primitive arrays of paired data.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class PairFactory
{

    /**
     * Returns a {@link DoubleVector} from a paired array of <code>double</code>.
     * 
     * @param pair the input pair
     * @return a {@link DoubleVector}
     * @throws PairException if the pair cannot be generated
     */

    public static DoubleVector getDoublePair(final double[] pair)
    {
        return new DoubleVector(pair);
    }

    /**
     * Returns a {@link BooleanVector} from a paired array of <code>boolean</code>.
     * 
     * @param pair the input pair
     * @return a {@link DoubleVector}
     * @throws PairException if the pair cannot be generated
     */

    public static BooleanVector getBooleanPair(final boolean[] pair)
    {
        return new BooleanVector(pair);
    }

}
