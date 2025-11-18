package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;

import lombok.Getter;
import lombok.Setter;

/**
 * A collection of time-=series values.
 */
@Setter
@Getter
public class TimeSeriesValues implements Serializable
{
    @Serial
    private static final long serialVersionUID = -23502269417368543L;

    /** Time-series values. */
    private TimeSeriesValue[] value;

    /** Qualifiers. */
    private Qualifier[] qualifier;

    /** Quality control levels. */
    private String[] qualityControlLevel;

    /** Method. */
    private Method[] method;

    /** Source. */
    private String[] source;

    /** Offsets. */
    private String[] offset;

    /** Samples. */
    private String[] sample;

    /** Censor codes. */
    private String[] censorCode;
}
