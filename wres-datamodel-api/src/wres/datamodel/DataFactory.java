package wres.datamodel;

/**
 * Factory class for creating instances of types in the interfaces in this jar.
 *
 * Calls the underlying implementation that is substituted at runtime. 
 *
 * This DataFactory is compiled against, the implementation needs to be on the
 * runtime classpath. Similar to compiling against slf4j.
 *
 * @author jesse
 *
 */
public class DataFactory
{
    private DataFactory()
    {
        // No construction, this is a static factory class.
    }

    public static PairOfDoubles pairOf(double first, double second)
    {
        return DataFactoryImpl.pairOf(first, second);
    }

    /**
     * Create a primitive pair of double, double[]
     * @param first the double
     * @param second the double array
     * @return the tuple
     */
    public static PairOfDoubleAndVectorOfDoubles pairOf(double first, double[] second)
    {
        return DataFactoryImpl.pairOf(first, second);
    }

    /**
     * Create a primitive tuple of double, double[] from boxed versions of same.
     *
     * @param first a boxed Double
     * @param second a boxed Double[]
     * @return the tuple of unboxed primitive, can use getKey() and getDoubles() on it.
     */
    public static PairOfDoubleAndVectorOfDoubles pairOf(Double first, Double[] second)
    {
        return DataFactoryImpl.pairOf(first, second);
    }
}
