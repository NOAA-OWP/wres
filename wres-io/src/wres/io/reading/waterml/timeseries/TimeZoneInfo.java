package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class TimeZoneInfo implements Serializable
{
    TimeZone defaultTimeZone;

    public TimeZone getDefaultTimeZone()
    {
        return defaultTimeZone;
    }

    public void setDefaultTimeZone( TimeZone defaultTimeZone )
    {
        this.defaultTimeZone = defaultTimeZone;
    }

    public TimeZone getDaylightSavingsTimeZone()
    {
        return daylightSavingsTimeZone;
    }

    public void setDaylightSavingsTimeZone( TimeZone daylightSavingsTimeZone )
    {
        this.daylightSavingsTimeZone = daylightSavingsTimeZone;
    }

    public Boolean getSiteUsesDaylightSavingsTime()
    {
        return siteUsesDaylightSavingsTime;
    }

    public void setSiteUsesDaylightSavingsTime( Boolean siteUsesDaylightSavingsTime )
    {
        this.siteUsesDaylightSavingsTime = siteUsesDaylightSavingsTime;
    }

    TimeZone daylightSavingsTimeZone;
    Boolean siteUsesDaylightSavingsTime;
}
