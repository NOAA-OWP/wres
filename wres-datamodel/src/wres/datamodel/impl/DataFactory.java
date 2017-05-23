package wres.datamodel.impl;

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
public class DataFactory implements wres.datamodel.DataFactory
{
    private static wres.datamodel.DataFactory INSTANCE = new DataFactory();

    /**
     * Get an instance with object creation methods.
     * @return the DataFactory instance
     */
    public static wres.datamodel.DataFactory instance()
    {
        return INSTANCE;
    }

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

    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf(final double first,
                                                 final double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf(final Double first,
                                                 final Double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    @Override
    public Pair<VectorOfDoubles, VectorOfDoubles> pairOf(final double[] first,
                                                         final double[] second)
    {
        return new Pair<VectorOfDoubles,VectorOfDoubles>()
        {
            @Override
            public VectorOfDoubles getItemOne()
            {
                return VectorOfDoublesImpl.of(first);
            }

            @Override
            public VectorOfDoubles getItemTwo()
            {
                return VectorOfDoublesImpl.of(second);
            }
        };
    }

    @Override
    public VectorOfDoubles vectorOf(final double[] vec)
    {
        return VectorOfDoublesImpl.of(vec);
    }

    @Override
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

    private DataFactory()
    {
        // prevent direct construction
    }
}
