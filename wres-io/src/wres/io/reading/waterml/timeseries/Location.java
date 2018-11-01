package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class Location implements Serializable
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
