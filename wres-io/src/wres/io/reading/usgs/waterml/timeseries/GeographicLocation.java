package wres.io.reading.usgs.waterml.timeseries;

public class GeographicLocation
{
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
