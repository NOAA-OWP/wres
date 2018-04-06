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

    public int getNumberOfPopulatedTimeSeries()
    {
        int populationCount = 0;
        for ( TimeSeries timeSeries : this.getTimeSeries())
        {
            if (timeSeries.isPopulated())
            {
                populationCount++;
            }
        }
        return populationCount;
    }

    QueryInfo queryInfo;
    TimeSeries[] timeSeries;
}
