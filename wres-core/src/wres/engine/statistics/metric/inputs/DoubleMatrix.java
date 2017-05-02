package wres.engine.statistics.metric.inputs;

import java.util.Objects;

import gov.noaa.wres.datamodel.Dataset;

/**
 * A mutable dataset that comprises a matrix of <code>double</code> values.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DoubleMatrix implements Dataset<double[][]>
{
    /**
     * The values.
     */

    private final double[][] values;

    /**
     * Build a matrix of <code>double</code> values.
     * 
     * @param values the double values
     */

    public DoubleMatrix(final double[][] values)
    {
        Objects.requireNonNull(values, "Provide non-null input for the double matrix.");
        this.values = values;
    }

    @Override
    public final double[][] getValues()
    {
        return values;
    }

    @Override
    public int size()
    {
        int size = 0;
        for(final double[] d: values)
        {
            size += d.length;
        }
        return size;
    }

}
