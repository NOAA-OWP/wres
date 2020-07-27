package wres.io.geography.wrds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

@JsonIgnoreProperties( ignoreUnknown = true )
public class WrdsLocation
{
    private final String nwmFeatureId;
    private final String usgsSiteCode;
    private final String nwsLid;
    private final String huc;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public WrdsLocation( @JsonProperty( "nwm_feature_id" )
                         String nwmFeatureId,
                         @JsonProperty( "usgs_site_code" )
                         String usgsSiteCode,
                         @JsonProperty( "nws_lid" )
                         String nwsLid,
                         @JsonProperty( "huc" )
                         String huc )
    {
        this.nwmFeatureId = nwmFeatureId;
        this.usgsSiteCode = usgsSiteCode;
        this.nwsLid = nwsLid;
        this.huc = huc;
    }

    public String getNwmFeatureId()
    {
        return this.nwmFeatureId;
    }

    public String getUsgsSiteCode()
    {
        return this.usgsSiteCode;
    }

    public String getNwsLid()
    {
        return this.nwsLid;
    }

    public String getHuc()
    {
        return this.huc;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "nwmFeatureId", nwmFeatureId )
                .append( "usgsSiteCode", usgsSiteCode )
                .append( "nwsLid", nwsLid )
                .append( "huc", huc )
                .toString();
    }
}
