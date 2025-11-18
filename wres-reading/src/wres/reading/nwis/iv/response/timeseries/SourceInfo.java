package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

import wres.reading.nwis.iv.response.query.QueryNote;

/**
 * Information about a source.
 */
@Setter
@Getter
public class SourceInfo implements Serializable
{
    @Serial
    private static final long serialVersionUID = 2257802722287031705L;

    /** Site name. */
    private String siteName;

    /** Site codes. */
    private SiteCode[] siteCode;

    /** Time zone. */
    private TimeZoneInfo timeZoneInfo;

    /** Location. */
    private Location geoLocation;

    /** Query notes. */
    private QueryNote[] note;

    /** Site types. */
    private String[] siteType;

    /** Site properties. */
    private SiteProperty[] siteProperty;
}
