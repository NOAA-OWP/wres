package wres.io.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;

import wres.io.reading.waterml.query.QueryNote;

/**
 * Information about a source.
 */
public class SourceInfo implements Serializable
{
    @Serial
    private static final long serialVersionUID = 2257802722287031705L;

    /** Site name. */
    private String siteName;
    /** Site codes. */
    private SiteCode[] siteCode;
    /** Time zone. */
    private TimeZoneInfo timeZoneInfo;
    /** Location. */
    private Location geoLocation;
    /** Query notes. */
    private QueryNote[] note;
    /** Site types. */
    private String[] siteType;
    /** Site properties. */
    private SiteProperty[] siteProperty;

    /**
     * @return the site name.
     */
    public String getSiteName()
    {
        return siteName;
    }

    /**
     * Sets the site name.
     * @param siteName the site name
     */
    public void setSiteName( String siteName )
    {
        this.siteName = siteName;
    }

    /**
     * @return the site code
     */
    public SiteCode[] getSiteCode()
    {
        return siteCode;
    }

    /**
     * Sets the site code.
     * @param siteCode the site code
     */
    public void setSiteCode( SiteCode[] siteCode )
    {
        this.siteCode = siteCode;
    }

    /**
     * @return the time zone information
     */
    public TimeZoneInfo getTimeZoneInfo()
    {
        return timeZoneInfo;
    }

    /**
     * Sets the time zone information.
     * @param timeZoneInfo the time zone information
     */
    public void setTimeZoneInfo( TimeZoneInfo timeZoneInfo )
    {
        this.timeZoneInfo = timeZoneInfo;
    }

    /**
     * @return the geographic location
     */
    public Location getGeoLocation()
    {
        return geoLocation;
    }

    /**
     * Sets the geographic location.
     * @param geoLocation the geographic location
     */
    public void setGeoLocation( Location geoLocation )
    {
        this.geoLocation = geoLocation;
    }

    /**
     * @return the notes
     */
    public QueryNote[] getNote()
    {
        return note;
    }

    /**
     * Sets the notes.
     * @param note the notes
     */
    public void setNote( QueryNote[] note )
    {
        this.note = note;
    }

    /**
     * @return the site type
     */
    public String[] getSiteType()
    {
        return siteType;
    }

    /**
     * Sets the site type.
     * @param siteType the site type
     */
    public void setSiteType( String[] siteType )
    {
        this.siteType = siteType;
    }

    /**
     * @return the site property
     */
    public SiteProperty[] getSiteProperty()
    {
        return siteProperty;
    }

    /**
     * Sets the site property.
     * @param siteProperty the site property
     */
    public void setSiteProperty( SiteProperty[] siteProperty )
    {
        this.siteProperty = siteProperty;
    }
}
