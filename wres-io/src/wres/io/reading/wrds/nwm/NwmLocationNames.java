package wres.io.reading.wrds.nwm;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmLocationNames
{
    private final int nwmFeatureId;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmLocationNames( @JsonProperty( "nwm_feature_id" )
                             int nwmFeatureId )
    {
        this.nwmFeatureId = nwmFeatureId;
    }

    public int getNwmFeatureId()
    {
        return this.nwmFeatureId;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "nwmFeatureId", nwmFeatureId )
                .toString();
    }
}
