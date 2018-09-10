package wres.io.reading.wrds;

import wres.util.Strings;

public class ForecastUnits
{
    // Use this rather than 'getFlow' because that will probably be renamed.
    public String getUnitName()
    {
        if( Strings.hasValue(this.flow))
        {
            return this.flow;
        }
        else if (Strings.hasValue( this.streamflow ))
        {
            return this.streamflow;
        }

        return this.stage;
    }

    public String getFlow()
    {
        return flow;
    }

    public void setFlow( String flow )
    {
        this.flow = flow;
    }
    public void setStreamflow(String streamflow)
    {
        this.streamflow = streamflow;
    }

    public void setStage( String stage )
    {
        this.stage = stage;
    }

    public String getStreamflow()
    {
        return streamflow;
    }

    public String getStage()
    {
        return stage;
    }

    String flow;
    String streamflow;
    String stage;
}
