package wres.io.reading.waterml;

import wres.io.reading.waterml.query.QueryInfo;
import wres.io.reading.waterml.timeseries.TimeSeries;

public class ResponseValue
{
    public QueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    public void setQueryInfo( QueryInfo queryInfo )
    {
        this.queryInfo = queryInfo;
    }

    public TimeSeries[] getTimeSeries()
    {
        return timeSeries;
    }

    public void setTimeSeries( TimeSeries[] timeSeries )
    {
        this.timeSeries = timeSeries;
    }

    QueryInfo queryInfo;
    TimeSeries[] timeSeries;
}
