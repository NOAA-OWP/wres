package wres.io.reading.waterml.query;

import java.io.Serializable;

public class QueryNote implements Serializable
{
    private static final long serialVersionUID = -5115638777040755540L;
 
    String value;
    String title;
    
    public String getValue()
    {
        return value;
    }

    public void setValue( String value )
    {
        this.value = value;
    }

    public String getTitle()
    {
        return title;
    }

    public void setTitle( String title )
    {
        this.title = title;
    }
}
