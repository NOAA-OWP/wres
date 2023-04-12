package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type of threshold.
 * @author James Brown
 */
public enum ThresholdType
{
    /** Probability threshold. */
    @JsonProperty( "probability" ) PROBABILITY( "probability" ),
    /** Probability classifier threshold. */
    @JsonProperty( "classifier" ) PROBABILITY_CLASSIFIER( "classifier" ),
    /** Value threshold. */
    @JsonProperty( "value" ) VALUE( "value" );

    /** The string name. */
    private final String stringName;

    /**
     * Creates an instance with a name.
     * @param stringName the name
     */
    ThresholdType( String stringName )
    {
        this.stringName = stringName;
    }

    /**
     * @return whether the threshold type is a probability
     */
    public boolean isProbability()
    {
        return this.name()
                   .startsWith( "PR" );
    }

    @Override
    public String toString()
    {
        return stringName;
    }
}