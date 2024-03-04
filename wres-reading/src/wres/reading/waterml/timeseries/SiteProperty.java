package wres.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;

/**
 * A site property.
 */
public class SiteProperty implements Serializable
{
    @Serial
    private static final long serialVersionUID = 7149841725947418537L;

    /** Value. */
    private String value;
    /** Name. */
    private String name;

    /**
     * @return the value
     */
    public String getValue()
    {
        return value;
    }

    /**
     * Sets the value.
     * @param value the value
     */
    public void setValue( String value )
    {
        this.value = value;
    }

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name.
     * @param name the name
     */
    public void setName( String name )
    {
        this.name = name;
    }
}
