package wres.io.reading.wrds;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import wres.util.Strings;

/**
 * Data point that contains a value and an associated time.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class DataPoint
{
    private OffsetDateTime time;
    private String flow;
    private Double value;

    /**
     * Sets the flow.
     * @param flow the flow
     */
    public void setFlow( String flow )
    {
        this.flow = flow;
    }

    /**
     * Gets the flow string.
     * @return the flow
     */
    public String getFlow()
    {
        return flow;
    }

    /**
     * @return the value
     */
    public Double getValue()
    {
        if ( this.value == null && this.flow != null )
        {
            return Double.parseDouble( this.flow );
        }

        return value;
    }

    /**
     * @return the time
     */
    public OffsetDateTime getTime()
    {
        return time;
    }

    /**
     * Sets the time.
     * @param time the time
     */
    public void setTime( String time )
    {
        if ( Strings.hasValue( time ) )
        {
            this.time = OffsetDateTime.parse( time );
        }
    }

    /**
     * Sets the value.
     * @param value the value
     */
    public void setValue( Double value )
    {
        this.value = value;
    }

}
