package wres.io.reading.waterml.variable;

import java.io.Serial;
import java.io.Serializable;

/**
 * Variable options.
 */
public class VariableOptions implements Serializable
{
    @Serial
    private static final long serialVersionUID = -3592017658139874856L;
    /** Option. */
    private Option[] option;

    /**
     * @return the options
     */
    public Option[] getOption()
    {
        return option;
    }

    /**
     * Sets the options.
     * @param option the options
     */
    public void setOption( Option[] option )
    {
        this.option = option;
    }
}
