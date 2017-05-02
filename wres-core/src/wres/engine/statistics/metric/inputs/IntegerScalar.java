package wres.engine.statistics.metric.inputs;

import gov.noaa.wres.datamodel.Dataset;

/**
 * Class for storing an integer scalar value.
 * 
 * @author james.brown@hydrosolved.com
 */

public class IntegerScalar implements Dataset<Integer>
{

    /**
     * The value.
     */

    private final int value;

    /**
     * Build a vector of <code>int</code> values.
     * 
     * @param value the int value
     */

    public IntegerScalar(final int value)
    {
        this.value = value;
    }

    @Override
    public Integer getValues()
    {
        return value;
    }

    /**
     * Returns the unboxed value.
     * 
     * @return the primitive
     */

    public int valueOf()
    {
        return value;
    }

    @Override
    public int size()
    {
        return 1;
    }
}
