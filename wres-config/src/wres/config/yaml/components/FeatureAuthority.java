package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationFactory;

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
        return DeclarationFactory.getFriendlyName( this.name() );
    }
}
