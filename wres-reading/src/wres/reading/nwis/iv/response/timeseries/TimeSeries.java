package wres.reading.nwis.iv.response.timeseries;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import lombok.Getter;
import lombok.Setter;

import wres.reading.nwis.iv.response.variable.Variable;

/**
 * A time-series.
 */
@Setter
@Getter
public class TimeSeries implements Serializable
{
    @Serial
    private static final long serialVersionUID = -5581319084334610655L;

    /** Source info. */
    private SourceInfo sourceInfo;

    /** Variable. */
    private Variable variable;

    /** Time-series values. */
    private TimeSeriesValues[] values;

    /** Name. */
    private String name;

    /**
     * @return true if there are time-series values, false otherwise
     */
    public boolean isPopulated()
    {
        if ( Objects.isNull( values ) || this.values.length == 0 )
        {
            return false;
        }

        for ( TimeSeriesValues v : this.getValues() )
        {
            if ( v.getValue().length > 0 )
            {
                return true;
            }
        }

        return false;
    }
}
