package wres.io.reading.usgs.waterml.timeseries;

public class Location
{
    public GeographicLocation getGeogLocation()
    {
        return geogLocation;
    }

    public void setGeogLocation( GeographicLocation geogLocation )
    {
        this.geogLocation = geogLocation;
    }

    public String[] getLocalSiteXY()
    {
        return localSiteXY;
    }

    public void setLocalSiteXY( String[] localSiteXY )
    {
        this.localSiteXY = localSiteXY;
    }

    GeographicLocation geogLocation;
    String[] localSiteXY;
}
