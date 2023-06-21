package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationUtilities;

/**
 * Supported methods for generating baseline datasets from a data source.
 * @author James Brown
 */
public enum GeneratedBaselines
{
    /** Climatology. */
    @JsonProperty( "climatology" ) CLIMATOLOGY,
    /** Persistence. */
    @JsonProperty( "persistence" ) PERSISTENCE;

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }
}
