package wres.datamodel;

import java.util.List;

import wres.datamodel.TupleOfDoubles;
import wres.datamodel.TuplesOfDoubles;

public class DataFactoryImpl
{
    public static TupleOfDoubles tupleOf(double first, double second)
    {
        return new TupleOfDoubles()
        {
            @Override
            public double[] getTupleOfDoubles()
            {
                double[] tuple = { first, second };
                return tuple;
            }
            
        };
    }

    public static TuplesOfDoubles tuplesOf(List<TupleOfDoubles> tuples)
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
}
