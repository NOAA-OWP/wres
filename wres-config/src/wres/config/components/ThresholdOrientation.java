package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.statistics.generated.Threshold;

/**
 * The orientation of the data to which the threshold applies.
 * @author James Brown
 */
public enum ThresholdOrientation
{
    /** Observed or left orientation. */
    @JsonProperty( "observed" ) LEFT( "observed" ),
    /** Predicted or right orientation. */
    @JsonProperty( "predicted" ) RIGHT( "predicted" ),
    /** Observed and predicted or left and right. */
    @JsonProperty( "observed and predicted" ) LEFT_AND_RIGHT( "observed and predicted" ),
    /** Any predicted or right value (e.g., within an ensemble). */
    @JsonProperty( "any predicted" ) ANY_RIGHT( "any predicted" ),
    /** Observed or left and any predicted or right value (e.g., within an ensemble). */
    @JsonProperty( "observed and any predicted" ) LEFT_AND_ANY_RIGHT( "observed and any predicted" ),
    /** Mean of right or predicted value (e.g., of an ensemble). */
    @JsonProperty( "predicted mean" ) RIGHT_MEAN( "predicted mean" ),
    /** Left and mean right or predicted value (e.g., of an ensemble). */
    @JsonProperty( "observed and predicted mean" ) LEFT_AND_RIGHT_MEAN( "observed and predicted mean" );

    /** The string name. */
    private final String stringName;

    /**
     * Creates an instance with a name.
     * @param stringName the name
     */
    ThresholdOrientation( String stringName )
    {
        this.stringName = stringName;
    }

    /**
     * Creates a canonical representation of the threshold orientation.
     * @return the canonical representation
     */

    public Threshold.ThresholdDataType canonical()
    {
        return Threshold.ThresholdDataType.valueOf( this.name() );
    }

    @Override
    public String toString()
    {
        return this.stringName;
    }
}