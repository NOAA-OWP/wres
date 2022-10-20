package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class TimeZone implements Serializable
{
    private static final long serialVersionUID = 6454995437183101190L;
    
    String zoneOffset;
    String zoneAbbreviation;
    
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
}
