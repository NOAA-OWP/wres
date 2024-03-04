package wres.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;

/**
 * A geographic location.
 */
public class GeographicLocation implements Serializable
{
    @Serial
    private static final long serialVersionUID = -6702537011475320084L;

    /** Spatial reference system. */
    private String srs;
    /** Latitude. */
    private Double latitude;
    /** Longitude. */
    private Double longitude;

    /**
     * @return the spatial reference frame
     */
    public String getSrs()
    {
        return srs;
    }

    /**
     * Sets the spatial reference frame.
     * @param srs the spatial reference frame
     */
    public void setSrs( String srs )
    {
        this.srs = srs;
    }

    /**
     * @return the latitude
     */
    public Double getLatitude()
    {
        return latitude;
    }

    /**
     * Sets the latitude.
     * @param latitude the latitude
     */
    public void setLatitude( Double latitude )
    {
        this.latitude = latitude;
    }

    /**
     * @return the longitude
     */
    public Double getLongitude()
    {
        return longitude;
    }

    /**
     * Sets the longitude.
     * @param longitude the longitude
     */
    public void setLongitude( Double longitude )
    {
        this.longitude = longitude;
    }
}
