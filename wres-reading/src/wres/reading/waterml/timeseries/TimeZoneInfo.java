package wres.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;

/**
 * Time zone information.
 */
public class TimeZoneInfo implements Serializable
{
    @Serial
    private static final long serialVersionUID = -7260060863961221920L;

    /** Default time zone. */
    private TimeZone defaultTimeZone;
    /** Daylight saving time zone. */
    private TimeZone daylightSavingsTimeZone;
    /** Whether the site uses daylight saving. */
    private boolean siteUsesDaylightSavingsTime;

    /**
     * @return the default time zone
     */
    public TimeZone getDefaultTimeZone()
    {
        return defaultTimeZone;
    }

    /**
     * Sets the default time zone.
     * @param defaultTimeZone the default time zone
     */
    public void setDefaultTimeZone( TimeZone defaultTimeZone )
    {
        this.defaultTimeZone = defaultTimeZone;
    }

    /**
     * @return the daylight saving time zone
     */
    public TimeZone getDaylightSavingsTimeZone()
    {
        return daylightSavingsTimeZone;
    }

    /**
     * Sets the daylight saving time zone.
     * @param daylightSavingsTimeZone the daylight saving time zone
     */
    public void setDaylightSavingsTimeZone( TimeZone daylightSavingsTimeZone )
    {
        this.daylightSavingsTimeZone = daylightSavingsTimeZone;
    }

    /**
     * @return whether the site uses daylight saving time
     */
    public boolean getSiteUsesDaylightSavingsTime()
    {
        return siteUsesDaylightSavingsTime;
    }

    /**
     * Sets whether the site uses daylight saving time
     * @param siteUsesDaylightSavingsTime whether the site uses daylight savings time
     */
    public void setSiteUsesDaylightSavingsTime( boolean siteUsesDaylightSavingsTime )
    {
        this.siteUsesDaylightSavingsTime = siteUsesDaylightSavingsTime;
    }
}
