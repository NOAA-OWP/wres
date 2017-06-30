package wres.datamodel;

/**
 * Provides methods for construction of common types.
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 */
public final class DataFactory
{

    public static PairOfDoubles pairOf(final double first, final double second)
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

    public static PairOfBooleans pairOf(final boolean first, final boolean second)
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

    public static PairOfDoubleAndVectorOfDoubles pairOf(final double first, final double[] second)
    {
        return SafePairOfDoubleAndVectorOfDoubles.of(first, second);
    }

    public static PairOfDoubleAndVectorOfDoubles pairOf(final Double first, final Double[] second)
    {
        return SafePairOfDoubleAndVectorOfDoubles.of(first, second);
    }

    public static Pair<VectorOfDoubles, VectorOfDoubles> pairOf(final double[] first, final double[] second)
    {
        return new Pair<VectorOfDoubles, VectorOfDoubles>()
        {
            @Override
            public VectorOfDoubles getItemOne()
            {
                return SafeVectorOfDoubles.of(first);
            }

            @Override
            public VectorOfDoubles getItemTwo()
            {
                return SafeVectorOfDoubles.of(second);
            }
        };
    }

    public static VectorOfDoubles vectorOf(final double[] vec)
    {
        return SafeVectorOfDoubles.of(vec);
    }

    public static VectorOfDoubles vectorOf(final Double[] vec)
    {
        return SafeVectorOfDoubles.of(vec);
    }

    public static VectorOfBooleans vectorOf(final boolean[] vec)
    {
        return SafeVectorOfBooleans.of(vec);
    }

    public static MatrixOfDoubles matrixOf(final double[][] vec)
    {
        return SafeMatrixOfDoubles.of(vec);
    }

    /**
     * Prevent direct construction.
     */

    private DataFactory()
    {
        // prevent direct construction
    }    
    
}
