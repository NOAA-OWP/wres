package wres.configcontrol.datamodel.attribute;

import java.util.Iterator;

import ucar.ma2.ArrayInt;

/**
 * {@link Axis} designed to store ensemble member indices, which are int instances.
 * 
 * @author Hank.Herr
 */
public class EnsembleAxis extends ArrayInt implements Axis<Integer>
{

    /**
     * @param len The number of member indices to store.
     */
    public EnsembleAxis(final int len)
    {
        super(new int[]{len});
    }

    /**
     * Calls {@link #getInt(int)}.
     */
    public int getMemberIndex(final int index)
    {
        return this.getInt(index);
    }

    /**
     * Calls {@link #setInt(int, int)}.
     */
    public void setMemberIndex(final int index, final int value)
    {
        this.setInt(index, value);
    }

    /**
     * Wrapper calls {@link #getMemberIndex(int)} after confirming only one index was provided.
     */
    @Override
    public Integer getValue(final int... indices)
    {
        if(indices.length != 1)
        {
            throw new IllegalArgumentException("Too many indices passed in; expected 1.");
        }
        return getMemberIndex(indices[0]);
    }

    /**
     * Wrapper calls {@link #setMemberIndex(int, int)} after confirming only one index was provided.
     */
    @Override
    public void setValue(final Integer value, final int... indices)
    {
        if(indices.length != 1)
        {
            throw new IllegalArgumentException("Too many indices passed in; expected 1.");
        }
        setMemberIndex(indices[0], value);
    }

    @Override
    public Iterator iterator()
    {
        return new UnidataArrayIterable<Integer>(this).iterator();
    }
}
