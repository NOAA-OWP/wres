package wres.io.reading.waterml;

import java.io.Serial;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import wres.io.reading.waterml.query.QueryInfo;
import wres.io.reading.waterml.timeseries.TimeSeries;

/**
 * A response value.
 */
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
     * @return the query information
     */
    public QueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    /**
     * Sets the query information.
     * @param queryInfo the query information
     */
    public void setQueryInfo( QueryInfo queryInfo )
    {
        this.queryInfo = queryInfo;
    }

    /**
     * @return the time-series
     */
    public TimeSeries[] getTimeSeries()
    {
        return timeSeries;
    }

    /**
     * Sets the time-series.
     * @param timeSeries the time-series
     */
    public void setTimeSeries( TimeSeries[] timeSeries )
    {
        this.timeSeries = timeSeries;
    }

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
