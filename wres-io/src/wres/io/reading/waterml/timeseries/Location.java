package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class Location implements Serializable
{
    private static final long serialVersionUID = 1907762874171212088L;
    
    GeographicLocation geogLocation;
    String[] localSiteXY;
    
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
}
