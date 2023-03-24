package wres.config.yaml.components;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.serializers.ValuesSerializer;

/**
 * A values filter.
 * @param minimum the smallest value permitted
 * @param maximum the largest value permitted
 * @param belowMinimum the value to assign when values are below the minimum
 * @param aboveMaximum the value to assign when values are above the maximum
 */
@RecordBuilder
@JsonSerialize( using = ValuesSerializer.class )
public record Values( @JsonProperty( "minimum" ) Double minimum,
                      @JsonProperty( "maximum" ) Double maximum,
                      @JsonProperty( "below_minimum" ) Double belowMinimum,
                      @JsonProperty( "above_maximum" ) Double aboveMaximum )
{
    /**
     * Sets the default values.
     * @param minimum the minimum
     * @param maximum the maximum
     * @param belowMinimum the value to assign when below the minimum
     * @param aboveMaximum the value to assign when above the maximum
     */
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
