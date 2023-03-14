package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Enumeration for cross-pairing.
 * @author James Brown
 */
public enum CrossPair
{
    /** Exact cross-pairing. */
    @JsonProperty( "exact" ) EXACT,
    /** Fuzzy cross-pairing. */
    @JsonProperty( "fuzzy" ) FUZZY,
}
