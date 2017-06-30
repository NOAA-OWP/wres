package wres.datamodel;

import java.util.stream.Stream;

/**
 * A Safe way to share a double array between threads. It is cloned on the way in, and cloned on the way out. This way,
 * if the original array is mutated after construction, the clone during construction prevents surprise. Likewise, the
 * clone during get prevents state being leaked to other classes. There does not yet seem to be a performance penalty.
 */
class SafeVectorOfDoubles implements VectorOfDoubles
{
    private final double[] doubles;

    private SafeVectorOfDoubles(final double[] doubles)
    {
        this.doubles = doubles.clone();
    }

    static VectorOfDoubles of(final double[] doubles)
    {
        return new SafeVectorOfDoubles(doubles);
    }

    static VectorOfDoubles of(final Double[] doubles)
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
}
