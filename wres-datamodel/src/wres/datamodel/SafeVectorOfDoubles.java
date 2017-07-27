package wres.datamodel;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A Safe way to share a double array between threads. It is cloned on the way in, and cloned on the way out. This way,
 * if the original array is mutated after construction, the clone during construction prevents surprise. Likewise, the
 * clone during get prevents state being leaked to other classes. There does not yet seem to be a performance penalty.
 */
public class SafeVectorOfDoubles implements VectorOfDoubles
{
    private final double[] doubles;

    private SafeVectorOfDoubles(final double[] doubles)
    {
        this.doubles = doubles.clone();
    }

    public static VectorOfDoubles of(final double[] doubles)
    {
        return new SafeVectorOfDoubles(doubles);
    }

    public static VectorOfDoubles of(final Double[] doubles)
    {
        return of(Stream.of(doubles).mapToDouble(Double::doubleValue).toArray());
    }

    @Override
    public double[] getDoubles()
    {
        return doubles.clone();
    }

    @Override
    public int size()
    {
        return doubles.length;
    }

    @Override public int compareTo(VectorOfDoubles other)
    {
        double[] otherDoubles = other.getDoubles();
        return compareDoubleArray(this.getDoubles(), otherDoubles);
    }

    /**
     * Consistent comparison of double arrays, first checks count of elements,
     * next goes through values.
     *
     * If first has fewer values, return -1, if first has more values, return 1.
     *
     * If value count is equal, go through in order until an element is less
     * or greater than another. If all values are equal, return 0.
     *
     * @param first
     * @param second
     * @return -1 if first is less than second, 0 if equal, 1 otherwise.
     */
    public static int compareDoubleArray(final double[] first,
                                         final double[] second)
    {
        // this one has fewer elements
        if (first.length < second.length)
        {
            return -1;
        }
        // this one has more elements
        else if (first.length > second.length)
        {
            return 1;
        }
        // compare values until we diverge
        else // assumption here is lengths are equal
        {
            for (int i = 0; i < first.length; i++)
            {
                int safeComparisonResult = Double.compare(first[i], second[i]);
                if (safeComparisonResult != 0)
                {
                    return safeComparisonResult;
                }
            }
            // all values were equal
            return 0;
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other instanceof VectorOfDoubles)
        {
            VectorOfDoubles otherVec = (VectorOfDoubles) other;
            return 0 == compareDoubleArray(this.getDoubles(), otherVec.getDoubles());
        }
        else
        {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(this.getDoubles());
    }
}
