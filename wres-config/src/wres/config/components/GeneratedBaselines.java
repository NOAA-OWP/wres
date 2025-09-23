package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.DeclarationUtilities;

/**
 * Supported methods for generating baseline datasets from a data source.
 * @author James Brown
 */
public enum GeneratedBaselines
{
    /** Climatology. */
    @JsonProperty( "climatology" ) CLIMATOLOGY( true ),
    /** Persistence. */
    @JsonProperty( "persistence" ) PERSISTENCE( false );
    /** Whether the generated baseline is an ensemble or single-valued. */
    private final boolean isEnsemble;

    /**
     * @return true if the generated baseline is an ensemble, false for single-valued
     */

    public boolean isEnsemble()
    {
        return this.isEnsemble;
    }

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }

    /**
     * Creates an instance.
     * @param isEnsemble is true if the generated baseline is an ensemble, false for single-valued
     */

    GeneratedBaselines( boolean isEnsemble )
    {
        this.isEnsemble = isEnsemble;
    }
}
