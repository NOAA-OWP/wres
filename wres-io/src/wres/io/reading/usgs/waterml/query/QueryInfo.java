package wres.io.reading.usgs.waterml.query;

public class QueryInfo
{
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

    String queryURL;
    QueryCriteria criteria;
    QueryNote[] note;
}
