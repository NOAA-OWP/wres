package wres.datamodel;

import java.util.Arrays;
import java.util.StringJoiner;
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
        StringJoiner s = new StringJoiner(",",
                                          "key: " + getItemOne() + " value: [",
                                          "]");
        for (final double d : getItemTwo())
        {
            s.add(Double.toString(d));
        }
        return s.toString();
    }

    @Override public int compareTo(
            PairOfDoubleAndVectorOfDoubles other)
    {
        // if the instances are the same...
        if (this == other)
        {
            return 0;
        }
        else if (this.getItemOne() == other.getItemOne())
        {
            // this one has fewer elements
            if (this.getItemTwo().length < other.getItemTwo().length)
            {
                return -1;
            }
            // this one has more elements
            else if (this.getItemTwo().length > other.getItemTwo().length)
            {
                return 1;
            }
            // compare values until we diverge
            else // assumption here is lengths are equal
            {
                for (int i = 0; i < this.getItemTwo().length; i++)
                {
                    if (this.getItemTwo()[i] != other.getItemTwo()[i])
                    {
                        return Double.compare(this.getItemTwo()[i],
                                              other.getItemTwo()[i]);
                    }
                }
                // all values were equal
                return 0;
            }
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
}
