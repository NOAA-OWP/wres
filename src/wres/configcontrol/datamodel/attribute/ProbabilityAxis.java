package wres.configcontrol.datamodel.attribute;

import java.util.Iterator;

import ucar.ma2.ArrayDouble;

/**
 * {@link Axis} designed to store probabilities, which are double instances in [0, 1].
 * 
 * @author Hank.Herr
 */
public class ProbabilityAxis extends ArrayDouble implements Axis<Double>
{

    /**
     * @param len The number of probabilities to be stored in this axis.
     */
    public ProbabilityAxis(final int len)
    {
        super(new int[]{len});
    }

    /**
     * Calls {@link #getDouble(int)}.
     */
    public double getProbability(final int index)
    {
        return this.getDouble(index);
    }

    /**
     * Calls {@link #setDouble(int, double)}.
     * 
     * @param value Must be between 0 and 1 or an {@link IllegalArgumentException} will be thrown.
     */
    public void setProbability(final int index, final double value)
    {
        if((value < 0.0) || (value >= 1.0))
        {
            throw new IllegalArgumentException("Value provided is not a valid probability within [0,1].");
        }
        this.setDouble(index, value);
    }

    /**
     * Wrapper calls {@link #setProbability(int, double)} after confirming only one index was provided.
     */
    @Override
    public void setValue(final Double value, final int... indices)
    {
        if(indices.length != 1)
        {
            throw new IllegalArgumentException("Too many indices passed in; expected 1.");
        }
        setProbability(indices[0], value);
    }

    /**
     * Wrapper calls {@link #getProbability(int)} after confirming only one index was provided.
     */
    @Override
    public Double getValue(final int... indices)
    {
        if(indices.length != 1)
        {
            throw new IllegalArgumentException("Too many indices passed in; expected 1.");
        }
        return getProbability(indices[0]);
    }

    @Override
    public Iterator iterator()
    {
        return new UnidataArrayIterable<Double>(this).iterator();
    }

}
