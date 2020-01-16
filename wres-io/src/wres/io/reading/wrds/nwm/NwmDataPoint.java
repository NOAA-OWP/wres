package wres.io.reading.wrds.nwm;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmDataPoint
{
    private final Instant time;
    private final double value;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmDataPoint( @JsonProperty( "time" )
                         @JsonFormat( shape = JsonFormat.Shape.STRING,
                                      pattern = "uuuuMMdd'T'HHX" )
                         Instant time,
                         @JsonProperty( "value" )
                         double value )
    {
        this.time = time;
        this.value = value;
    }

    public Instant getTime()
    {
        return this.time;
    }

    public double getValue()
    {
        return this.value;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "time", time )
                .append( "value", value )
                .toString();
    }
}
