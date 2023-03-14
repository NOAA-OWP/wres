package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * A values filter.
 * @param minimum the smallest value permitted
 * @param maximum the largest value permitted
 * @param belowMinimum the value to assign when values are below the minimum
 * @param aboveMaximum the value to assign when values are above the maximum
 */
@RecordBuilder
public record Values( @JsonProperty( "minimum" ) Double minimum,
                      @JsonProperty( "maximum" ) Double maximum,
                      @JsonProperty( "below_minimum" ) Double belowMinimum,
                      @JsonProperty( "above_maximum" ) Double aboveMaximum )
{
    // Set default values
    public Values
    {
        if ( Objects.isNull( minimum ) )
        {
            minimum = Double.NEGATIVE_INFINITY;
        }

        if ( Objects.isNull( maximum ) )
        {
            maximum = Double.POSITIVE_INFINITY;
        }

        if ( Objects.isNull( belowMinimum ) )
        {
            belowMinimum = Double.NaN;
        }

        if ( Objects.isNull( aboveMaximum ) )
        {
            aboveMaximum = Double.NaN;
        }
    }
}
