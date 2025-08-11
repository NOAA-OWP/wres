package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

import wres.statistics.generated.Threshold;

/**
 * The orientation of the data to which the threshold applies.
 * @author James Brown
 */
public enum ThresholdOrientation
{
    /** Observed or left orientation. */
    @JsonProperty( "observed" ) OBSERVED( "observed" ),
    /** Predicted or right orientation. */
    @JsonProperty( "predicted" ) PREDICTED( "predicted" ),
    /** Observed and predicted or left and right. */
    @JsonProperty( "observed and predicted" ) OBSERVED_AND_PREDICTED( "observed and predicted" ),
    /** Any predicted or right value (e.g., within an ensemble). */
    @JsonProperty( "any predicted" ) ANY_PREDICTED( "any predicted" ),
    /** Observed or left and any predicted or right value (e.g., within an ensemble). */
    @JsonProperty( "observed and any predicted" ) OBSERVED_AND_ANY_PREDICTED( "observed and any predicted" ),
    /** Mean of right or predicted value (e.g., of an ensemble). */
    @JsonProperty( "predicted mean" ) PREDICTED_MEAN( "predicted mean" ),
    /** Left and mean right or predicted value (e.g., of an ensemble). */
    @JsonProperty( "observed and predicted mean" ) OBSERVED_AND_PREDICTED_MEAN( "observed and predicted mean" );

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
        return Threshold.ThresholdDataType.valueOf( this.toString()
                                                        .replace( " ", "_" )
                                                        .toUpperCase() );
    }
//
//    /**
//     * Creates a ThresholdOrientation based on the stringName of the enum
//     * @param input the stringName of the ThresholdOrientation
//     * @return A ThresholdOrientation
//     */
//    public static ThresholdOrientation fromString(String input)
//    {
//        for ( ThresholdOrientation orientation : ThresholdOrientation.values() )
//        {
//            if (orientation.toString().equalsIgnoreCase( input ) )
//            {
//                return orientation;
//            }
//        }
//        return null;
//    }

    @Override
    public String toString()
    {
        return this.stringName;
    }
}