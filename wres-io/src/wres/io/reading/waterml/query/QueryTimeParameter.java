package wres.io.reading.waterml.query;

import java.io.Serializable;

public class QueryTimeParameter implements Serializable
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
