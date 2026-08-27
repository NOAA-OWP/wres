package wres.reading.wrds.nwm_legacy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

/**
 * A NWM location.
 */
@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmLocation
{
    /** The NWM location names. */
    private final NwmLocationNames nwmLocationNames;

    /** The NWM members. */
    private final NwmMember[] nwmMembers;

    /**
     * Creates an instance.
     * @param nwmLocationNames the NWM location names
     * @param nwmMembers the NWM members
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmLocation( @JsonProperty( "names" )
                        NwmLocationNames nwmLocationNames,
                        @JsonProperty( "members" )
                        NwmMember[] nwmMembers )
    {
        this.nwmLocationNames = nwmLocationNames;
        this.nwmMembers = nwmMembers;
    }

}
