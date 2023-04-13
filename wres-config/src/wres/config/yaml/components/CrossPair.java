package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationUtilities;

/**
 * Enumeration for cross-pairing.
 * @author James Brown
 */
public enum CrossPair
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
