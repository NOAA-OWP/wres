package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationUtilities;

/**
 * Enumeration for the cross-pairing method.
 * @author James Brown
 */
public enum CrossPairMethod
{
    /** Exact cross-pairing. */
    @JsonProperty( "exact" )
    EXACT,
    /** Fuzzy cross-pairing. */
    @JsonProperty( "fuzzy" )
    FUZZY;

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }
}
