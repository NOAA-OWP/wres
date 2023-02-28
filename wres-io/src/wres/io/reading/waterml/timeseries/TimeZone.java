package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

/**
 * A time zone.
 */
public class TimeZone implements Serializable
{
    private static final long serialVersionUID = 6454995437183101190L;

    /** Time zone. */
    private String zoneOffset;
    /** Time zone abbreviation. */
    private String zoneAbbreviation;

    /**
     * @return the time zone offset
     */
    public String getZoneOffset()
    {
        return zoneOffset;
    }

    /**
     * Sets the time zone offset.
     * @param zoneOffset the time zone offset
     */
    public void setZoneOffset( String zoneOffset )
    {
        this.zoneOffset = zoneOffset;
    }

    /**
     * @return the time zone abbreviation
     */
    public String getZoneAbbreviation()
    {
        return zoneAbbreviation;
    }

    /**
     * Sets the time zone abbreviation.
     * @param zoneAbbreviation the time zone abbreviation
     */
    public void setZoneAbbreviation( String zoneAbbreviation )
    {
        this.zoneAbbreviation = zoneAbbreviation;
    }
}
