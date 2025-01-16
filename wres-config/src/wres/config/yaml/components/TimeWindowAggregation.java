package wres.config.yaml.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Identifies how time windows or "events" should be aggregated across datasets.
 *
 * @author James Brown
 */
public enum TimeWindowAggregation
{
    /** Form the minimum period across each set of intersecting periods.*/
    @JsonProperty( "minimum" )
    MINIMUM,

    /** Form the maximum period across each set of intersecting periods.*/
    @JsonProperty( "maximum" )
    MAXIMUM,

    /** Form the average period spanned by each set of intersecting periods.*/
    @JsonProperty( "average" )
    AVERAGE
}