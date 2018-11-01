package wres.io.reading.waterml.query;

import java.io.Serializable;

public class QueryNote implements Serializable
{
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

    String value;
    String title;
}
