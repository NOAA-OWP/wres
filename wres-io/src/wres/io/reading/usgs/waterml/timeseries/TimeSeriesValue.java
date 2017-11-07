package wres.io.reading.usgs.waterml.timeseries;

public class TimeSeriesValue
{
    String value;

    public String getValue()
    {
        return value;
    }

    public void setValue( String value )
    {
        this.value = value;
    }

    public String[] getQualifiers()
    {
        return qualifiers;
    }

    public void setQualifiers( String[] qualifiers )
    {
        this.qualifiers = qualifiers;
    }

    public String getDateTime()
    {
        return dateTime;
    }

    public void setDateTime( String dateTime )
    {
        this.dateTime = dateTime;
    }

    String[] qualifiers;
    String dateTime;
}
