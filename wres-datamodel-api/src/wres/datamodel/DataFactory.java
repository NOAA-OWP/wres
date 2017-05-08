package wres.datamodel;

import java.util.List;

public class DataFactory
{
    public static TupleOfDoubles tupleOf(double first, double second)
    {
        return DataFactoryImpl.tupleOf(first, second);
    }

    public static TuplesOfDoubles tuplesOf(List<TupleOfDoubles> tuples)
    {
        return DataFactoryImpl.tuplesOf(tuples);
    }
}
