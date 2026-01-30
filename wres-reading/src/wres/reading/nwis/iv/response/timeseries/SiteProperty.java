package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A site property.
 */
@Setter
@Getter
public class SiteProperty implements Serializable
{
    @Serial
    private static final long serialVersionUID = 7149841725947418537L;

    /** Value. */
    private String value;

    /** Name. */
    private String name;
}
