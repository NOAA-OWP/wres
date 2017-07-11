package wres.datamodel;

import java.util.stream.Stream;

public class SafePairOfDoubleAndVectorOfDoubles
implements PairOfDoubleAndVectorOfDoubles
{
    private final double itemOne;
    private final double[] itemTwo;

    private SafePairOfDoubleAndVectorOfDoubles(final double key, final double[] doubles)
    {
        this.itemOne = key;
        this.itemTwo = doubles.clone();
    }

    public static PairOfDoubleAndVectorOfDoubles of(final double key, final double[] doubles)
    {
        return new SafePairOfDoubleAndVectorOfDoubles(key, doubles);
    }

    public static PairOfDoubleAndVectorOfDoubles of(final Double key, final Double[] doubles)
    {
        final double[] unboxedDoubles = Stream.of(doubles)
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
        final StringBuilder s = new StringBuilder();
        s.append("key: ");
        s.append(getItemOne());
        s.append(" ");
        s.append("value: [ ");
        for (final double d : getItemTwo())
        {
            s.append(d);
            s.append(" ");
        }
        s.append("]");
        return s.toString();
    }
}
