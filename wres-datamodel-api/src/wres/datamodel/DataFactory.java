package wres.datamodel;

/**
 * An abstract factory for creating datasets.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 * @version 0.1
 * @since 0.1
 */

public interface DataFactory
{
    /**
     * Create a pair of primitive doubles.
     * 
     * @param first the first value
     * @param second the second value
     * @return the paired values
     */
    PairOfDoubles pairOf(double first, double second);

    /**
     * Create a primitive pair of double, double[]
     * 
     * @param first the double
     * @param second the double array
     * @return the paired values
     */
    PairOfDoubleAndVectorOfDoubles pairOf(double first, double[] second);

    /**
     * Create a primitive pair of double, double[] from boxed versions of same.
     *
     * @param first a boxed Double
     * @param second a boxed Double[]
     * @return the paired values
     */
    PairOfDoubleAndVectorOfDoubles pairOf(Double first, Double[] second);

    /**
     * Create a pair of double[], double[].
     *
     * This is to model the case of ensemble vs ensemble. Therefore, the size
     * of the vectors is not necessarily equal.
     *
     * @param first double[] of any length
     * @param second double[] of any length
     * @return the pair
     */
    Pair<VectorOfDoubles,VectorOfDoubles> pairOf(double[] first, double[] second);

    /**
     * Create a VectorOfDoubles using a primitive double[]
     */
    VectorOfDoubles vectorOf(double[] vec);

    /**
     * Create a VectorOfBooleans using a primitive boolean[]
     * 
     * @param vec the vector of values
     */
    VectorOfBooleans vectorOf(boolean[] vec);
}
