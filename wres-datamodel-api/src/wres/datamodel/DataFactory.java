package wres.datamodel;

import java.util.List;

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

    /**
     * High level helper to get the typical paired ensemble data structure.
     * 
     * @param observation
     * @param forecast
     * @return
     */
    public static PairOfOneObsManyFcMembers pairOf(Double observation, Double[] forecast)
    {
        return DataFactoryImpl.pairOf(observation, forecast);
    }

    public static PairOfDoubles tupleOf(double first, double second)
    {
        return DataFactoryImpl.tupleOf(first, second);
    }

    public static PairsOfDoubles tuplesOf(List<PairOfDoubles> tuples)
    {
        return DataFactoryImpl.tuplesOf(tuples);
    }

    /**
     * Get a primitive tuple of double, double[]
     * @param first the double
     * @param second the double array
     * @return the tuple
     */
    public static PairOfDoubleAndVectorOfDoubles tupleOf(double first, double[] second)
    {
        return DataFactoryImpl.tupleOf(first, second);
    }

    /**
     * Get a primitive tuple of double, double[] from boxed versions of same.
     *
     * @param first a boxed Double
     * @param second a boxed Double[]
     * @return the tuple of unboxed primitive, can use getKey() and getDoubles() on it.
     */
    public static PairOfDoubleAndVectorOfDoubles tupleOf(Double first, Double[] second)
    {
        return DataFactoryImpl.tupleOf(first, second);
    }
}
