package wres.datamodel;

import java.util.List;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.PairsOfDoubles;

class DataFactoryImpl
{
    static PairOfOneObsManyFcMembers pairOf(Double observation, Double[] forecast)
    {
        // EnsemblePair is a friendlier name for TupleOfDoubleAndDoubleArray
        return (PairOfOneObsManyFcMembers) PairOfDoubleAndVectorOfDoublesImpl.of(observation,
                                                                     forecast);
    }

    static PairOfDoubles tupleOf(double first, double second)
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

    static PairsOfDoubles tuplesOf(List<PairOfDoubles> tuples)
    {
        return new PairsOfDoubles()
        {
            @Override
            public List<PairOfDoubles> getTuplesOfDoubles()
            {
                return tuples;
            }
        };
    }

    static PairOfDoubleAndVectorOfDoubles tupleOf(double first, double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

    static PairOfDoubleAndVectorOfDoubles tupleOf(Double first, Double[] second)
    {
        return PairOfDoubleAndVectorOfDoublesImpl.of(first, second);
    }

}
