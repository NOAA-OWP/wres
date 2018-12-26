package wres.io.reading.wrds;

import java.time.OffsetDateTime;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import wres.util.Strings;

@JsonIgnoreProperties( ignoreUnknown = true )
public class DataPoint
{
    public void setFlow( String flow )
    {
        this.flow = flow;
    }

    public String getFlow()
    {
        return flow;
    }

    public Double getValue()
    {
        if (this.value == null && this.flow != null)
        {
            return Double.parseDouble( this.flow );
        }

        return value;
    }

    public OffsetDateTime getTime()
    {
        return time;
    }

    public void setTime( String time )
    {
        if ( Strings.hasValue(time))
        {
            this.time = OffsetDateTime.parse(time);
        }
    }

    public void setValue( Double value )
    {
        this.value = value;
    }

    OffsetDateTime time;
    String flow;
    Double value;
}
