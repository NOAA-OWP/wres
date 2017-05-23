package wres.datamodel.impl;

import wres.datamodel.VectorOfDoubles;

class VectorOfDoublesImpl implements VectorOfDoubles
{
    private final double[] doubles;

    private VectorOfDoublesImpl(double[] doubles)
    {
        this.doubles = doubles.clone();
    }

    static VectorOfDoubles of(final double[] doubles)
    {
        return new VectorOfDoublesImpl(doubles);
    }

    @Override
    public double[] getDoubles()
    {
        return doubles.clone();
    }
}
