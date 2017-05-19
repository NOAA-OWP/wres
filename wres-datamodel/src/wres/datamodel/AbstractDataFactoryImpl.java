package wres.datamodel;

/**
 * A concrete implementation of a factory for creating datasets.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class AbstractDataFactoryImpl implements AbstractDataFactory
{

    /**
     * An instance of a {@link AbstractDataFactoryImpl}. TEMPORARY NOTE: if preferred, allow construction, hide this
     * static instance and implement getInstance() to return it. However, this is more fluent.
     */

    public static final AbstractDataFactoryImpl of = new AbstractDataFactoryImpl();

    /**
     * Create a pair of primitive doubles.
     * 
     * @param first the first value
     * @param second the second value
     * @return the paired values
     */
    @Override
    public PairOfDoubles pairOf(final double first, final double second)
    {
        return new PairOfDoubles()
        {
            @Override
            public double getItemOne()
            {
                return first;
            }

            @Override
            public double getItemTwo()
            {
                return second;
            }
        };
    }

    /**
     * Create a pair of primitive booleans
     * 
     * @param first the first value
     * @param second the second value
     * @return the paired values
     */
    @Override
    public PairOfBooleans pairOf(final boolean first, final boolean second)
    {
        return new PairOfBooleans()
        {
            @Override
            public boolean getItemOne()
            {
                return first;
            }

            @Override
            public boolean getItemTwo()
            {
                return second;
            }
        };
    }

    /**
     * Create a primitive pair of double, double[]
     * 
     * @param first the double
     * @param second the double array
     * @return the tuple
     */
    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf(final double first, final double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    /**
     * Create a primitive pair of double, double[] from boxed versions of same.
     *
     * @param first a boxed Double
     * @param second a boxed Double[]
     * @return the tuple of unboxed primitive, can use getKey() and getDoubles() on it.
     */
    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf(final Double first, final Double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    /**
     * Create a VectorOfDoubles using a primitive double[]
     */
    @Override
    public VectorOfDoubles vectorOf(final double[] vec)
    {
        return new VectorOfDoubles()
        {
            @Override
            public double[] getDoubles()
            {
                return vec.clone();
            }
        };
    }

    /**
     * Create a VectorOfBooleans using a primitive boolean[]
     * 
     * @param vec the
     */
    public VectorOfBooleans vectorOf(final boolean[] vec)
    {
        return new VectorOfBooleans()
        {
            @Override
            public boolean[] getBooleans()
            {
                return vec.clone();
            }
        };
    }

    /**
     * Avoid construction. Reference the static member {@link #of} instead.
     */

    private AbstractDataFactoryImpl()
    {

    }

}
