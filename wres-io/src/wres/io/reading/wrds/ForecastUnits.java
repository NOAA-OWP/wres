package wres.io.reading.wrds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import wres.util.Strings;

/**
 * The forecast units.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class ForecastUnits
{
    private String flow;
    private String streamflow;
    private String stage;

    /**
     * @return the unit name
     */
    public String getUnitName()
    {
        if ( Strings.hasValue( this.flow ) )
        {
            return this.flow;
        }
        else if ( Strings.hasValue( this.streamflow ) )
        {
            return this.streamflow;
        }

        return this.stage;
    }

    /**
     * @return the flow
     */
    public String getFlow()
    {
        return flow;
    }

    /**
     * Sets the flow.
     * @param flow the flow
     */
    public void setFlow( String flow )
    {
        this.flow = flow;
    }

    /**
     * Sets the streamflow.
     * @param streamflow the streamflow
     */
    public void setStreamflow( String streamflow )
    {
        this.streamflow = streamflow;
    }

    /**
     * Sets the stage.
     * @param stage the stage
     */
    public void setStage( String stage )
    {
        this.stage = stage;
    }

    /**
     * @return the streamflow
     */
    public String getStreamflow()
    {
        return streamflow;
    }

    /**
     * @return the stage
     */
    public String getStage()
    {
        return stage;
    }
}
