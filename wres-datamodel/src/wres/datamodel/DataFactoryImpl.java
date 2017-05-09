package wres.datamodel;

import java.util.List;

import wres.datamodel.TupleOfDoubles;
import wres.datamodel.TuplesOfDoubles;

class DataFactoryImpl
{
    static TupleOfDoubles tupleOf(double first, double second)
    {
        return new TupleOfDoubles()
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

    static TuplesOfDoubles tuplesOf(List<TupleOfDoubles> tuples)
    {
        return new TuplesOfDoubles()
        {
            @Override
            public List<TupleOfDoubles> getTuplesOfDoubles()
            {
                return tuples;
            }
        };
    }

    static TupleOfDoubleAndDoubleArray tupleOf(double first, double[] second)
    {
        return TupleOfDoubleAndDoubleArrayImpl.of(first, second);
    }

    static TupleOfDoubleAndDoubleArray tupleOf(Double first, Double[] second)
    {
        return TupleOfDoubleAndDoubleArrayImpl.of(first, second);
    }

}
