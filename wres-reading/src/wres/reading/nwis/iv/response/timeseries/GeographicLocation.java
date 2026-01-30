package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A geographic location.
 */
@Setter
@Getter
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
}
