package wres.io.reading.wrds.nwm;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmLocation
{
    private final NwmLocationNames nwmLocationNames;
    private final NwmMember[] nwmMembers;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmLocation( @JsonProperty( "names" )
                        NwmLocationNames nwmLocationNames,
                        @JsonProperty( "members" )
                        NwmMember[] nwmMembers )
    {
        this.nwmLocationNames = nwmLocationNames;
        this.nwmMembers = nwmMembers;
    }

    public NwmLocationNames getNwmLocationNames()
    {
        return this.nwmLocationNames;
    }

    public NwmMember[] getNwmMembers()
    {
        return this.nwmMembers;
    }
}
