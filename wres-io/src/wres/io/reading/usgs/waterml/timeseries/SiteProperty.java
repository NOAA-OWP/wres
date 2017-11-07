package wres.io.reading.usgs.waterml.timeseries;

public class SiteProperty
{
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

    String value;
    String name;
}
