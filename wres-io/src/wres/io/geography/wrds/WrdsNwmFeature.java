package wres.io.geography.wrds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

@JsonIgnoreProperties( ignoreUnknown = true )
public class WrdsNwmFeature
{
    private final String nwmFeatureId;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public WrdsNwmFeature( @JsonProperty( "nwm_feature_id" )
                           String nwmFeatureId )
    {
        this.nwmFeatureId = nwmFeatureId;
    }

    public String getNwmFeatureId()
    {
        return this.nwmFeatureId;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "nwmFeatureId", nwmFeatureId )
                .toString();
    }
}
