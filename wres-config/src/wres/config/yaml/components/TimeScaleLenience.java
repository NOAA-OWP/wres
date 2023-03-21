package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Enumeration for the lenience to be applied when rescaling data.
 * @author James Brown
 */
public enum TimeScaleLenience
{
    /** Apply lenience to all sides of data. */
    @JsonProperty( "all" ) ALL,
    /** Apply lenience to no sides of data. */
    @JsonProperty( "none" ) NONE,
    /** Apply lenience to the left data only. */
    @JsonProperty( "observed" ) LEFT,
    /** Apply lenience to the right data only. */
    @JsonProperty( "predicted" ) RIGHT,
    /** Apply lenience to the baseline data only. */
    @JsonProperty( "baseline" ) BASELINE,
    /** Apply lenience to the right and baseline data only. */
    @JsonProperty( "predicted and baseline" ) RIGHT_AND_BASELINE,
    /** Apply lenience to the left and baseline data only. */
    @JsonProperty( "observed and baseline" ) LEFT_AND_BASELINE,
    /** Apply lenience to the left and right data only. */
    @JsonProperty( "observed and predicted" ) LEFT_AND_RIGHT
}
