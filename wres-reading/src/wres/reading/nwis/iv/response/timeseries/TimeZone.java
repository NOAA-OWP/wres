package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A time zone.
 */
@Setter
@Getter
public class TimeZone implements Serializable
{
    @Serial
    private static final long serialVersionUID = 6454995437183101190L;

    /** Time zone. */
    private String zoneOffset;

    /** Time zone abbreviation. */
    private String zoneAbbreviation;
}
