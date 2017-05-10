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

    static PairOfDoubleAndVectorOfDoubles pairOf(double first, double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    static PairOfDoubleAndVectorOfDoubles pairOf(Double first, Double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }
}
