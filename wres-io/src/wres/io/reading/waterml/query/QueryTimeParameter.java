package wres.io.reading.waterml.query;

import java.io.Serializable;

/**
 * A query time parameter.
 */

public class QueryTimeParameter implements Serializable
{
    private static final long serialVersionUID = 8007018441738383263L;

    /** Start time. */
    private String beginDateTime;
    /** End time. */
    private String endDateTime;

    /**
     * @return the beginning time
     */
    public String getBeginDateTime()
    {
        return beginDateTime;
    }

    /**
     * Sets the beginning time.
     * @param beginDateTime the beginning time
     */
    public void setBeginDateTime( String beginDateTime )
    {
        this.beginDateTime = beginDateTime;
    }

    /**
     * @return the end time
     */
    public String getEndDateTime()
    {
        return endDateTime;
    }

    /**
     * Sets the end time.
     * @param endDateTime the end time
     */
    public void setEndDateTime( String endDateTime )
    {
        this.endDateTime = endDateTime;
    }
}
