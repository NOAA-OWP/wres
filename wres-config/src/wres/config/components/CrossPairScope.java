package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.DeclarationUtilities;

/**
 * Enumeration for the scope of cross-pairing.
 * @author James Brown
 */
public enum CrossPairScope
{
    /** Only cross-pair time-series for corresponding geographic features. */
    @JsonProperty( "within features" )
    WITHIN_FEATURES,
    /** Cross-pair all time-series, regardless of geographic feature. */
    @JsonProperty( "across features" )
    ACROSS_FEATURES;

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }
}
