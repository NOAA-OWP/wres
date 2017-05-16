package wres.datamodel;

import wres.datamodel.PairOfDoubles;

class DataFactoryImpl
{
    static PairOfDoubles pairOf(double first, double second)
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

    static PairOfBooleans pairOf(boolean first, boolean second)
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

    static PairOfDoubleAndVectorOfDoubles pairOf(double first, double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    static PairOfDoubleAndVectorOfDoubles pairOf(Double first, Double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    static VectorOfDoubles vectorOf(double[] vec)
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

    static VectorOfBooleans vectorOf(boolean[] vec)
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
}
