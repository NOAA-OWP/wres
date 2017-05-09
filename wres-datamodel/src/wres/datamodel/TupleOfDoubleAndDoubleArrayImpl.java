package wres.datamodel;

import java.util.stream.Stream;

class TupleOfDoubleAndDoubleArrayImpl
implements TupleOfDoubleAndDoubleArray
{
    private final double itemOne;
    private final double[] itemTwo;

    private TupleOfDoubleAndDoubleArrayImpl(double key, double[] doubles)
    {
        this.itemOne = key;
        this.itemTwo = doubles;
    }

    static TupleOfDoubleAndDoubleArray of(double key, double[] doubles)
    {
        return new TupleOfDoubleAndDoubleArrayImpl(key, doubles);
    }

    static TupleOfDoubleAndDoubleArray of(Double key, Double[] doubles)
    {
        double[] unboxedDoubles = Stream.of(doubles)
                                        .mapToDouble(Double::doubleValue)
                                        .toArray();
        return new TupleOfDoubleAndDoubleArrayImpl(key.doubleValue(), unboxedDoubles);
    }

    @Override
    public double[] getItemTwo()
    {
        return itemTwo;
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
