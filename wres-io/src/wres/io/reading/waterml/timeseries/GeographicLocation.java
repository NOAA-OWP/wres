package wres.io.reading.waterml.timeseries;

import java.io.Serializable;

public class GeographicLocation implements Serializable
{
    private static final long serialVersionUID = -6702537011475320084L;
    
    String srs;

    public String getSrs()
    {
        return srs;
    }

    public void setSrs( String srs )
    {
        this.srs = srs;
    }

    public Double getLatitude()
    {
        return latitude;
    }

    public void setLatitude( Double latitude )
    {
        this.latitude = latitude;
    }

    public Double getLongitude()
    {
        return longitude;
    }

    public void setLongitude( Double longitude )
    {
        this.longitude = longitude;
    }

    Double latitude;
    Double longitude;
}
