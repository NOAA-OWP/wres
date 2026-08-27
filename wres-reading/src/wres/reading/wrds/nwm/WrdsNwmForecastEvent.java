package wres.reading.wrds.nwm;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>One (time, value) pair within a single WRDS NWM ensemble member's time-series, i.e., one element of a
 * member's {@code "timeseries"} JSON array.
 *
 * @param validDatetime the valid datetime of the event
 * @param value the event value
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public record WrdsNwmForecastEvent( Instant validDatetime, double value )
{
    @JsonCreator
    public WrdsNwmForecastEvent( @JsonProperty( "valid_datetime" )
                                 @JsonFormat( shape = JsonFormat.Shape.STRING,
                                              pattern = "uuuu-MM-dd'T'HH:mm:ssX" )
                                 Instant validDatetime,
                                 @JsonProperty( "value" ) double value )
    {
        this.validDatetime = validDatetime;
        this.value = value;
    }
}
