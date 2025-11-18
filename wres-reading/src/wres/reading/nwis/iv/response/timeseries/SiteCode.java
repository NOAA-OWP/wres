package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A site code.
 */
@Setter
@Getter
public class SiteCode implements Serializable
{
    @Serial
    private static final long serialVersionUID = -7293845985686518859L;

    /** Value. */
    private String value;

    /** Network. */
    private String network;

    /** Agency code. */
    private String agencyCode;
}
