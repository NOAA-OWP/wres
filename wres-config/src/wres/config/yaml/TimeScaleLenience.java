package wres.config.yaml;

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
    @JsonProperty( "left" ) LEFT,
    /** Apply lenience to the right data only. */
    @JsonProperty( "right" ) RIGHT,
    /** Apply lenience to the baseline data only. */
    @JsonProperty( "baseline" ) BASELINE,
    /** Apply lenience to the right and baseline data only. */
    @JsonProperty( "right and baseline" ) RIGHT_AND_BASELINE,
    /** Apply lenience to the left and baseline data only. */
    @JsonProperty( "left and baseline" ) LEFT_AND_BASELINE,
    /** Apply lenience to the left and right data only. */
    @JsonProperty( "left and right" ) LEFT_AND_RIGHT;
}
