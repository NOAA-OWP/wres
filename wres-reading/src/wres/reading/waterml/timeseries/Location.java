package wres.reading.waterml.timeseries;

import java.io.Serial;
import java.io.Serializable;

/**
 * A location.
 */
public class Location implements Serializable
{
    @Serial
    private static final long serialVersionUID = 1907762874171212088L;

    /** Geographic location. */
    private GeographicLocation geogLocation;
    /** Coordinates. */
    private String[] localSiteXY;

    /**
     * @return the geographic location
     */
    public GeographicLocation getGeogLocation()
    {
        return geogLocation;
    }

    /**
     * Sets the geographic location.
     * @param geogLocation the geographic location
     */
    public void setGeogLocation( GeographicLocation geogLocation )
    {
        this.geogLocation = geogLocation;
    }

    /**
     * @return The site coordinates.
     */
    public String[] getLocalSiteXY()
    {
        return localSiteXY;
    }

    /**
     * Sets the site coordinates.
     * @param localSiteXY the site coordinates
     */
    public void setLocalSiteXY( String[] localSiteXY )
    {
        this.localSiteXY = localSiteXY;
    }
}
