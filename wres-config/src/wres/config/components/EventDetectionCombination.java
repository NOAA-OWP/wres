package wres.config.components;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Identifies how events should be combined across datasets.
 *
 * @author James Brown
 */
public enum EventDetectionCombination
{
    /** Form the union of all events.*/
    @JsonProperty( "union" )
    UNION,

    /** Form the intersection of events. Events are intersected if they overlap. */
    @JsonProperty( "intersection" )
    INTERSECTION
}