package wres.io.reading.waterml.query;

import java.io.Serializable;

public class QueryInfo implements Serializable
{
    private static final long serialVersionUID = 6672020602331767905L;
 
    String queryURL;
    QueryCriteria criteria;
    QueryNote[] note;
    
    public String getQueryURL()
    {
        return queryURL;
    }

    public void setQueryURL( String queryURL )
    {
        this.queryURL = queryURL;
    }

    public QueryCriteria getCriteria()
    {
        return criteria;
    }

    public void setCriteria( QueryCriteria criteria )
    {
        this.criteria = criteria;
    }

    public QueryNote[] getNote()
    {
        return note;
    }

    public void setNote( QueryNote[] note )
    {
        this.note = note;
    }
}
