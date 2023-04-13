package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.config.yaml.DeclarationUtilities;
import wres.statistics.generated.Threshold;

/**
 * Wraps the canonical {@link wres.statistics.generated.Threshold.ThresholdOperator} with user-friendly names.
 * @author James Brown
 */
public enum ThresholdOperator
{
    /** Greater than. */
    @JsonProperty( "greater" ) GREATER,
    /** Less than. */
    @JsonProperty( "less" ) LESS,
    /** Greater than or equal to. */
    @JsonProperty( "greater equal" ) GREATER_EQUAL,
    /** Less than or equal to. */
    @JsonProperty( "less equal" ) LESS_EQUAL,
    /** Equal to. */
    @JsonProperty( "equal" ) EQUAL;

    /**
     * @return the canonical representation
     */
    public Threshold.ThresholdOperator canonical()
    {
        return Threshold.ThresholdOperator.valueOf( this.name() );
    }

    @Override
    public String toString()
    {
        return DeclarationUtilities.fromEnumName( this.name() );
    }
}
