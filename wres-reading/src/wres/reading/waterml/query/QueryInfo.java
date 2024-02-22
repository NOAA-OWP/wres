package wres.reading.waterml.query;

import java.io.Serializable;

/**
 * Query information.
 */
public class QueryInfo implements Serializable
{
    private static final long serialVersionUID = 6672020602331767905L;

    /** Query URL. */
    private String queryURL;
    /** Criteria. */
    private QueryCriteria criteria;
    /** Query notes. */
    private QueryNote[] note;

    /**
     * @return the query URL
     */
    public String getQueryURL()
    {
        return queryURL;
    }

    /**
     * Sets the query URL.
     * @param queryURL the query URL
     */
    public void setQueryURL( String queryURL )
    {
        this.queryURL = queryURL;
    }

    /**
     * @return the criteria
     */
    public QueryCriteria getCriteria()
    {
        return criteria;
    }

    /**
     * Sets the criteria.
     * @param criteria the criteria
     */
    public void setCriteria( QueryCriteria criteria )
    {
        this.criteria = criteria;
    }

    /**
     * @return the notes
     */
    public QueryNote[] getNote()
    {
        return note;
    }

    /**
     * Sets the notes.
     * @param note the notes
     */
    public void setNote( QueryNote[] note )
    {
        this.note = note;
    }
}
