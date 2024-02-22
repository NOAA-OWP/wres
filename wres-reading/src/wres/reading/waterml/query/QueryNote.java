package wres.reading.waterml.query;

import java.io.Serializable;

/**
 * A query note.
 */
public class QueryNote implements Serializable
{
    private static final long serialVersionUID = -5115638777040755540L;

    /** Value. */
    private String value;
    /** Title. */
    private String title;

    /**
     * @return the value
     */
    public String getValue()
    {
        return value;
    }

    /**
     * Sets the value
     * @param value the value
     */
    public void setValue( String value )
    {
        this.value = value;
    }

    /**
     * @return the title
     */
    public String getTitle()
    {
        return title;
    }

    /**
     * Sets the title.
     * @param title the title
     */
    public void setTitle( String title )
    {
        this.title = title;
    }
}
