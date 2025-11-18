package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * Time zone information.
 */
@Setter
@Getter
public class TimeZoneInfo implements Serializable
{
    @Serial
    private static final long serialVersionUID = -7260060863961221920L;

    /** Default time zone. */
    private TimeZone defaultTimeZone;

    /** Daylight saving time zone. */
    private TimeZone daylightSavingsTimeZone;

    /** Whether the site uses daylight saving. */
    private boolean siteUsesDaylightSavingsTime;
}
