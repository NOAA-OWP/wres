package wres.engine.statistics.metric.inputs;

import java.util.Objects;

import wres.datamodel.VectorOfDoubles;

/**
 * A mutable dataset that comprises a vector of <code>double</code> values.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DoubleVector implements Dataset<double[]>, VectorOfDoubles
{
    /**
     * The values.
     */

    private final double[] values;

    /**
     * Build a vector of <code>double</code> values.
     * 
     * @param values the double values
     */

    public DoubleVector(final double[] values)
    {
        Objects.requireNonNull(values, "Provide non-null input for the double vector.");
        this.values = values;
    }

    @Override
    public final double[] getValues()
    {
        return values;
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public double[] getDoubles()
    {
        return values.clone();
    }

}
