package wres.engine.statistics.metric.inputs;

import gov.noaa.wres.datamodel.Dataset;

/**
 * Class for storing a double scalar value.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DoubleScalar implements Dataset<Double>
{

    /**
     * The value.
     */

    private final double value;

    /**
     * Build a vector of <code>double</code> values.
     * 
     * @param value the double value
     */

    public DoubleScalar(final double value)
    {
        this.value = value;
    }

    @Override
    public Double getValues()
    {
        return value;
    }

    /**
     * Returns the unboxed value.
     * 
     * @return the primitive
     */

    public double valueOf()
    {
        return value;
    }

    @Override
    public int size()
    {
        return 1;
    }
}
