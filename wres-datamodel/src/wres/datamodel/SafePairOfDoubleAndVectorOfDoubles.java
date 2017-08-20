package wres.datamodel;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Stream;

class SafePairOfDoubleAndVectorOfDoubles
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
        StringJoiner s = new StringJoiner(",",
                                          "key: " + getItemOne() + " value: [",
                                          "]");
        for (final double d : getItemTwo())
        {
            s.add(Double.toString(d));
        }
        return s.toString();
    }

    @Override public int compareTo(PairOfDoubleAndVectorOfDoubles other)
    {
        // if the instances are the same...
        if (this == other)
        {
            return 0;
        }
        else if (Double.compare(this.getItemOne(), other.getItemOne()) == 0)
        {
            return SafeVectorOfDoubles.compareDoubleArray(this.getItemTwo(),
                                                          other.getItemTwo());
        }
        else if (this.getItemOne() < other.getItemOne())
        {
            return -1;
        }
        else
        {
            return 1;
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other instanceof PairOfDoubleAndVectorOfDoubles)
        {
            PairOfDoubleAndVectorOfDoubles otherPair =
                    (PairOfDoubleAndVectorOfDoubles) other;
            return 0 == this.compareTo(otherPair);
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.getItemOne(), Arrays.hashCode(this.getItemTwo()));
    }
}
