package wres.datamodel;

import java.util.stream.Stream;

class TupleOfDoubleAndDoubleArrayImpl
implements TupleOfDoubleAndDoubleArray
{
    private final double key;
    private final double[] doubles;

    private TupleOfDoubleAndDoubleArrayImpl(double key, double[] doubles)
    {
        this.key = key;
        this.doubles = doubles;
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
    public double[] getDoubles()
    {
        return doubles;
    }

    @Override
    public double getKey()
    {
        return key;
    }

    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder();
        s.append("key: ");
        s.append(getKey());
        s.append(" ");
        s.append("value: [ ");
        for (double d : getDoubles())
        {
            s.append(d);
            s.append(" ");
        }
        s.append("]");
        return s.toString();
    }
}
