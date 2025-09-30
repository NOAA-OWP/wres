package wres.config.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Enumeration for cross-pairing.
 * @author James Brown
 */
public record CrossPair( @JsonProperty( "method" ) CrossPairMethod method,
                         @JsonProperty( "scope" ) CrossPairScope scope )
{
    /**
     * Set the defaults.
     * @param method the cross-pairing method.
     * @param scope the scope of cross-pairing.
     */
    public CrossPair
    {
        if ( Objects.isNull( method ) )
        {
            method = CrossPairMethod.EXACT;
        }
    }
}
