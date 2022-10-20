package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class SiteProperty implements Serializable
{
    private static final long serialVersionUID = 7149841725947418537L;
    
    String value;
    String name;
    
    public String getValue()
    {
        return value;
    }

    public void setValue( String value )
    {
        this.value = value;
    }

    public String getName()
    {
        return name;
    }

    public void setName( String name )
    {
        this.name = name;
    }
}
