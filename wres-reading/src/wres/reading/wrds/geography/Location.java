package wres.reading.wrds.geography;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.FeatureAuthority;

/**
 * Corresponds to the "identifiers" within an element in the list of "locations".
 * @author Hank Herr
 * @author James Brown
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public record Location( String nwmFeatureId, String usgsSiteCode, String nwsLid )
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Location.class );

    /**
     * Creates an instance.
     * @param nwmFeatureId the NWM feature ID
     * @param usgsSiteCode the USGS site code
     * @param nwsLid the NWS LID
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )  // Redefine default constructor for json creation
    public Location( @JsonProperty( "nwm_feature_id" )  // NOSONAR
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
     * @param location the collection of names to inspect
     * @return the correct name for the authority
     */
    public static String getNameForAuthority( FeatureAuthority featureAuthority, Location location )
    {
        if ( Objects.isNull( featureAuthority ) || featureAuthority == FeatureAuthority.CUSTOM )
        {
            // Probably not safe, in general, but maintaining this assumption in migrating from the old to the new
            // declaration language: #113677
            LOGGER.debug( "While inspecting WRDS location {}, discovered a feature authority of '{}' from which to "
                          + "determine the location name. Assuming that the NWS LID is required. This may not be a "
                          + "safe assumption!", location, featureAuthority );
            return location.nwsLid();
        }

        return switch ( featureAuthority )
                {
                    case NWS_LID -> location.nwsLid();
                    case USGS_SITE_CODE -> location.usgsSiteCode();
                    case NWM_FEATURE_ID -> location.nwmFeatureId();
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
