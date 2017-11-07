package wres.io.reading.usgs.waterml.query;

public class QueryTimeParameter
{
    public String getBeginDateTime()
    {
        return beginDateTime;
    }

    public void setBeginDateTime( String beginDateTime )
    {
        this.beginDateTime = beginDateTime;
    }

    public String getEndDateTime()
    {
        return endDateTime;
    }

    public void setEndDateTime( String endDateTime )
    {
        this.endDateTime = endDateTime;
    }

    String beginDateTime;
    String endDateTime;
}
