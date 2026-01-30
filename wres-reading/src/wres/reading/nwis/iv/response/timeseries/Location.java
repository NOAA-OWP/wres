package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A location.
 */
@Setter
@Getter
public class Location implements Serializable
{
    @Serial
    private static final long serialVersionUID = 1907762874171212088L;

    /** Geographic location. */
    private GeographicLocation geogLocation;

    /** Coordinates. */
    private String[] localSiteXY;
}
