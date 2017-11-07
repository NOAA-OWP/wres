package wres.io.reading.usgs.waterml.query;

public class QueryNote
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
