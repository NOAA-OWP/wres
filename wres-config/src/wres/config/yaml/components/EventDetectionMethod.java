package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationUtilities;

/**
 * Enumeration of the event detection method.
 * @author James Brown
 */
public enum EventDetectionMethod
{
    /** The method described in <a href="https://onlinelibrary.wiley.com/doi/10.1002/hyp.14405">
     * Regina and Ogden (2021)</a>.*/
    @JsonProperty( "regina-ogden" )
    REGINA_OGDEN,

    /** The default method. */
    @JsonProperty( "default" )
    DEFAULT;

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() )
                                   .replace( " ", "-" );
    }
}