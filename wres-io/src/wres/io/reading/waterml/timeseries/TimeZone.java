package wres.io.reading.waterml.timeseries;

public class TimeZone
{
    String zoneOffset;

    public String getZoneOffset()
    {
        return zoneOffset;
    }

    public void setZoneOffset( String zoneOffset )
    {
        this.zoneOffset = zoneOffset;
    }

    public String getZoneAbbreviation()
    {
        return zoneAbbreviation;
    }

    public void setZoneAbbreviation( String zoneAbbreviation )
    {
        this.zoneAbbreviation = zoneAbbreviation;
    }

    String zoneAbbreviation;
}
