package wres.io.reading.waterml.timeseries;

import wres.io.reading.waterml.query.QueryNote;

public class SourceInfo
{
    public String getSiteName()
    {
        return siteName;
    }

    public void setSiteName( String siteName )
    {
        this.siteName = siteName;
    }

    public SiteCode[] getSiteCode()
    {
        return siteCode;
    }

    public void setSiteCode( SiteCode[] siteCode )
    {
        this.siteCode = siteCode;
    }

    public TimeZoneInfo getTimeZoneInfo()
    {
        return timeZoneInfo;
    }

    public void setTimeZoneInfo( TimeZoneInfo timeZoneInfo )
    {
        this.timeZoneInfo = timeZoneInfo;
    }

    public Location getGeoLocation()
    {
        return geoLocation;
    }

    public void setGeoLocation( Location geoLocation )
    {
        this.geoLocation = geoLocation;
    }

    public QueryNote[] getNote()
    {
        return note;
    }

    public void setNote( QueryNote[] note )
    {
        this.note = note;
    }

    public String[] getSiteType()
    {
        return siteType;
    }

    public void setSiteType( String[] siteType )
    {
        this.siteType = siteType;
    }

    public SiteProperty[] getSiteProperty()
    {
        return siteProperty;
    }

    public void setSiteProperty( SiteProperty[] siteProperty )
    {
        this.siteProperty = siteProperty;
    }

    String siteName;
    SiteCode[] siteCode;
    TimeZoneInfo timeZoneInfo;
    Location geoLocation;
    QueryNote[] note;
    String[] siteType;
    SiteProperty[] siteProperty;
}
