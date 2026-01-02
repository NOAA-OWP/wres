package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;
import tools.jackson.databind.annotation.JsonSerialize;

import wres.config.serializers.TimeScaleLenienceSerializer;

/**
 * Enumeration for the lenience to be applied when rescaling data.
 * @author James Brown
 */
@JsonSerialize( using = TimeScaleLenienceSerializer.class )
public enum TimeScaleLenience
{
    /** Apply lenience to all sides of data. */
    @JsonProperty( "all" ) ALL( "all" ),
    /** Apply lenience to no sides of data. */
    @JsonProperty( "none" ) NONE( "none" ),
    /** Apply lenience to the left data only. */
    @JsonProperty( "observed" ) LEFT( "observed" ),
    /** Apply lenience to the right data only. */
    @JsonProperty( "predicted" ) RIGHT( "predicted" ),
    /** Apply lenience to the baseline data only. */
    @JsonProperty( "baseline" ) BASELINE( "baseline" ),
    /** Apply lenience to the right and baseline data only. */
    @JsonProperty( "predicted and baseline" ) RIGHT_AND_BASELINE( "predicted and baseline" ),
    /** Apply lenience to the left and baseline data only. */
    @JsonProperty( "observed and baseline" ) LEFT_AND_BASELINE( "observed and baseline" ),
    /** Apply lenience to the left and right data only. */
    @JsonProperty( "observed and predicted" ) LEFT_AND_RIGHT( "observed and predicted" );

    /** The string representation. */
    private final String stringName;

    /**
     * Creates an instance.
     * @param stringName the string name
     */
    TimeScaleLenience( String stringName )
    {
        this.stringName = stringName;
    }

    @Override
    public String toString()
    {
        return stringName;
    }
}
