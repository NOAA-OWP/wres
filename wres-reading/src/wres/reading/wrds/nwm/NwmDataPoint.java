package wres.reading.wrds.nwm;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A NWM data point.
 * @param time the valid time
 * @param value the value
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@JsonDeserialize( using = NwmDataPointDeserializer.class )
public record NwmDataPoint( @JsonProperty( "time" )
                            @JsonFormat( shape = JsonFormat.Shape.STRING,
                                         pattern = "uuuuMMdd'T'HHX" )
                            Instant time,
                            @JsonProperty( "value" )
                            double value )
{
    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "time", time )
                .append( "value", value )
                .toString();
    }
}
