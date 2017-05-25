package wres.datamodel;

import wres.datamodel.Pair;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.VectorOfDoubles;

/**
 * Provides methods for construction of common types.
 *
 * @author jesse
 * @author james.brown@hydrosolved.com
 *
 */
public class DataFactory
{
    private static final DataFactory INSTANCE = new DataFactory();

    private DataFactory()
    {
        // prevent direct construction
    }

    /**
     * Get an instance with object creation methods.
     * @return the DataFactory instance
     */
    public static wres.datamodel.DataFactory instance()
    {
        return INSTANCE;
    }

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

    public PairOfDoubleAndVectorOfDoubles pairOf(final double first,
                                                 final double[] second)
    {
        return SafePairOfDoubleAndVectorOfDoubles.of(first, second);
    }

    public PairOfDoubleAndVectorOfDoubles pairOf(final Double first,
                                                 final Double[] second)
    {
        return SafePairOfDoubleAndVectorOfDoubles.of(first, second);
    }

    public Pair<VectorOfDoubles, VectorOfDoubles> pairOf(final double[] first,
                                                         final double[] second)
    {
        return new Pair<VectorOfDoubles,VectorOfDoubles>()
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

    public VectorOfDoubles vectorOf(final double[] vec)
    {
        return SafeVectorOfDoubles.of(vec);
    }

    public VectorOfBooleans vectorOf(final boolean[] vec)
    {
        return SafeVectorOfBooleans.of(vec);
    }
}
