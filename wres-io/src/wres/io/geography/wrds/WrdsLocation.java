package wres.io.geography.wrds;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * For WRDS Location API 2.0 and older, this corresponds directly to an element in
 * the list of "locations".  For WRDS Location API 3.0 and later, this corresponds 
 * to the "identifiers" WITHIN an element in the list of "locations".  
 * @author Hank Herr
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public record WrdsLocation( String nwmFeatureId, String usgsSiteCode, String nwsLid )
{
    /**
     * Creates an instance.
     * @param nwmFeatureId the NWM feature ID
     * @param usgsSiteCode the USGS site code
     * @param nwsLid the NWS LID
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )  // Redefine default constructor for json creation
    public WrdsLocation( @JsonProperty( "nwm_feature_id" )  // NOSONAR
                         String nwmFeatureId,
                         @JsonProperty( "usgs_site_code" )
                         String usgsSiteCode,
                         @JsonProperty( "nws_lid" )
                         String nwsLid )
    {
        this.nwmFeatureId = nwmFeatureId;
        this.usgsSiteCode = usgsSiteCode;
        this.nwsLid = nwsLid;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "nwmFeatureId", nwmFeatureId )
                .append( "usgsSiteCode", usgsSiteCode )
                .append( "nwsLid", nwsLid )
                .toString();
    }
}
