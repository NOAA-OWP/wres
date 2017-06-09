package wres.datamodel;

import java.util.stream.Stream;

class SafePairOfDoubleAndVectorOfDoubles
implements PairOfDoubleAndVectorOfDoubles
{
    private final double itemOne;
    private final double[] itemTwo;

    private SafePairOfDoubleAndVectorOfDoubles(double key, double[] doubles)
    {
        this.itemOne = key;
        this.itemTwo = doubles.clone();
    }

    static PairOfDoubleAndVectorOfDoubles of(double key, double[] doubles)
    {
        return new SafePairOfDoubleAndVectorOfDoubles(key, doubles);
    }

    static PairOfDoubleAndVectorOfDoubles of(Double key, Double[] doubles)
    {
        double[] unboxedDoubles = Stream.of(doubles)
                                        .mapToDouble(Double::doubleValue)
                                        .toArray();
        return new SafePairOfDoubleAndVectorOfDoubles(key.doubleValue(), unboxedDoubles);
    }

    @Override
    public double[] getItemTwo()
    {
        return itemTwo.clone();
    }

    @Override
    public double getItemOne()
    {
        return itemOne;
    }

    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder();
        s.append("key: ");
        s.append(getItemOne());
        s.append(" ");
        s.append("value: [ ");
        for (double d : getItemTwo())
        {
            s.append(d);
            s.append(" ");
        }
        s.append("]");
        return s.toString();
    }
}
