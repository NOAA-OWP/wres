package wres.io.reading.waterml.query;

import java.io.Serializable;

public class QueryTimeParameter implements Serializable
{
    private static final long serialVersionUID = 8007018441738383263L;
 
    String beginDateTime;
    String endDateTime;
    
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
}
