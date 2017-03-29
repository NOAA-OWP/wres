package wres.engine.statistics.metric.inputs;

import java.util.Objects;

/**
 * An immutable dataset that comprises a vector of <code>boolean</code> values.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BooleanVector implements Dataset<boolean[]>
{
    /**
     * The values.
     */

    private final boolean[] values;

    /**
     * Build a vector of <code>boolean</code> values.
     * 
     * @param values the boolean values
     */

    public BooleanVector(final boolean[] values)
    {
        Objects.requireNonNull(values, "Provide non-null input for the boolean vector.");
        this.values = values;
    }

    @Override
    public final boolean[] getValues()
    {
        return values;
    }

    @Override
    public int size()
    {
        return values.length;
    }

}
