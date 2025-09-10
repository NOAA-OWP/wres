package wres.config.components;

import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.deserializers.TimePoolsDeserializer;
import wres.config.serializers.TimeIntervalSerializer;

/**
 * A sequence of time pools, which are bounded by a minimum and a maximum time for the overall evaluation.
 *
 * @param period the period or duration of each pool
 * @param frequency the spacing between pools
 * @param reverse is true to count backwards from the maximum time bound, false to count forwards from the minimum
 */
@RecordBuilder
@JsonSerialize( using = TimeIntervalSerializer.class )
@JsonDeserialize( using = TimePoolsDeserializer.class )
public record TimePools( Duration period,
                         Duration frequency,
                         boolean reverse )
{
    /**
     * Sets the default values.
     * @param period the period
     * @param frequency the frequency
     * @param reverse is true to count backwards from the maximum time bound, false to count forwards from the minimum
     */
    public TimePools
    {
        // If the period is non-zero, set the frequency equal to the period by default
        if ( Objects.isNull( frequency )
             && !Duration.ZERO.equals( period ) )
        {
            frequency = period;
        }
    }
}
