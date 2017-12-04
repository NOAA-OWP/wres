package wres.io.reading.waterml.timeseries;

public class TimeSeriesValue
{
    Double value;

    public Double getValue()
    {
        return value;
    }

    public void setValue( Double value )
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
