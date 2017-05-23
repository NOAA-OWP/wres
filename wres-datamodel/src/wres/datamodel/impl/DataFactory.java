package wres.datamodel.impl;

import java.util.concurrent.locks.ReentrantLock;

import net.jcip.annotations.GuardedBy;
import wres.datamodel.PairOfBooleans;
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
    @GuardedBy("LOCK")
    private static DataFactory INSTANCE;
    private static final ReentrantLock LOCK = new ReentrantLock();

    /**
     * Get an instance with object creation methods.
     * @return the DataFactory instance
     */
    public static DataFactory instance()
    {
        synchronized(LOCK)
        {
            if (INSTANCE == null)
            {
                INSTANCE = new DataFactory();
            }
        }
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
    public PairOfBooleans pairOf(final boolean first,
                                        final boolean second)
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
