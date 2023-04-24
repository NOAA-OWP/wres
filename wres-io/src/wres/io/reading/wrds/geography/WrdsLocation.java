package wres.io.reading.wrds.geography;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.FeatureAuthority;

/**
 * For WRDS Location API 2.0 and older, this corresponds directly to an element in
 * the list of "locations".  For WRDS Location API 3.0 and later, this corresponds 
 * to the "identifiers" WITHIN an element in the list of "locations".  
 * @author Hank Herr
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public record WrdsLocation( String nwmFeatureId, String usgsSiteCode, String nwsLid )
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsLocation.class );

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

    /**
     * Determines the feature name from the prescribed location and feature authority.
     * @param featureAuthority the feature authority
     * @param wrdsLocation the collection of names to inspect
     * @return the correct name for the authority
     */
    public static String getNameForAuthority( FeatureAuthority featureAuthority, WrdsLocation wrdsLocation )
    {
        if ( Objects.isNull( featureAuthority ) )
        {
            LOGGER.debug( "While inspecting WRDS location {}, discovered a null feature authority.", wrdsLocation );
            return null;
        }

        return switch ( featureAuthority )
                {
                    case NWS_LID -> wrdsLocation.nwsLid();
                    case USGS_SITE_CODE -> wrdsLocation.usgsSiteCode();
                    case NWM_FEATURE_ID -> wrdsLocation.nwmFeatureId();
                    default -> throw new UnsupportedOperationException( "This feature authority is not supported: "
                                                                        + featureAuthority
                                                                        + "." );
                };
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
