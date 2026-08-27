package wres.reading.wrds.nwm;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * <p>One ensemble member of a WRDS NWM forecast, i.e., one element of a {@link WrdsNwmForecastRecord}'s
 * {@code "forecast"} JSON array.
 *
 * <p>Whether the enclosing forecast record is an ensemble forecast (multiple members) or a single-valued forecast
 * (one member) is a property of the collection of members, not of any individual member, so that determination is
 * exposed via {@link WrdsNwmForecastRecord#isEnsemble()} rather than here.
 *
 * @param memberId the ensemble member identifier
 * @param events the member's time-series events, in the order supplied by the service
 * @author James Brown
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public record WrdsNwmForecastMember( int memberId, List<WrdsNwmForecastEvent> events )
{
    @JsonCreator
    public WrdsNwmForecastMember( @JsonProperty( "member_id" ) int memberId,
                                  @JsonProperty( "timeseries" ) List<WrdsNwmForecastEvent> events )
    {
        this.memberId = memberId;
        this.events = List.copyOf( Objects.requireNonNullElse( events, List.of() ) );
    }
}
