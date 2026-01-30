package wres.reading.nwis.iv.response;

import java.io.Serial;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;

import wres.reading.nwis.iv.response.query.QueryInfo;
import wres.reading.nwis.iv.response.timeseries.TimeSeries;

/**
 * A WaterML response value.
 */
@Setter
@Getter
public class ResponseValue implements Serializable
{
    @Serial
    private static final long serialVersionUID = -136902551774808505L;

    /** Query info. */
    @JsonIgnore
    private QueryInfo queryInfo;

    /** Time series. */
    private TimeSeries[] timeSeries;

    /**
     * @return the number of time-series available
     */
    public int getNumberOfPopulatedTimeSeries()
    {
        int populationCount = 0;
        for ( TimeSeries series : this.getTimeSeries() )
        {
            if ( series.isPopulated() )
            {
                populationCount++;
            }
        }
        return populationCount;
    }
}
