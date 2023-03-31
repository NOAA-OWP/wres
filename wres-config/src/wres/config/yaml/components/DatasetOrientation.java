package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The orientation of a dataset within an evaluation.
 * @author James Brown
 */
public enum DatasetOrientation
{
    /** A left or observed dataset. */
    @JsonProperty( "observed" ) LEFT( "observed" ),
    /** A right or predicted dataset. */
    @JsonProperty( "predicted" ) RIGHT( "predicted" ),
    /** A baseline dataset. */
    @JsonProperty( "baseline" ) BASELINE( "baseline" );

    private final String stringName;

    DatasetOrientation( String stringName )
    {
        this.stringName = stringName;
    }

    @Override
    public String toString()
    {
        return stringName;
    }
}
