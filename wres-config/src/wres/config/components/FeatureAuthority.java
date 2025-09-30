package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.DeclarationUtilities;

/**
 * Feature authorities.
 * @author James Brown
 */
public enum FeatureAuthority
{
    /** National Weather Service LID. */
    @JsonProperty( "nws lid" )
    NWS_LID,
    /** United States Geological Survey Site Code. */
    @JsonProperty( "usgs site code" )
    USGS_SITE_CODE,
    /** National Water Model Feature ID. */
    @JsonProperty( "nwm feature id" )
    NWM_FEATURE_ID,
    /** Custom feature authority. */
    @JsonProperty( "custom" )
    CUSTOM;

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }

    /**
     * Convenience method that returns a lower case representation of the enum {@link #name()}.
     * @return a lower case representation of {@link #name()}
     */

    public String nameLowerCase()
    {
        return this.name()
                   .toLowerCase();
    }

}
