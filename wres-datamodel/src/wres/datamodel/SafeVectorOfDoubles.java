package wres.datamodel;

import wres.datamodel.VectorOfDoubles;

class SafeVectorOfDoubles implements VectorOfDoubles
{
    private final double[] doubles;

    private SafeVectorOfDoubles(double[] doubles)
    {
        this.doubles = doubles.clone();
    }

    static VectorOfDoubles of(final double[] doubles)
    {
        return new SafeVectorOfDoubles(doubles);
    }

    @Override
    public double[] getDoubles()
    {
        return doubles.clone();
    }
}
