package wres.config.yaml.components;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.deserializers.LeadTimeIntervalDeserializer;
import wres.config.yaml.deserializers.TimeIntervalDeserializer;
import wres.config.yaml.serializers.ChronoUnitSerializer;
import wres.config.yaml.serializers.LeadTimeIntervalSerializer;
import wres.config.yaml.serializers.TimeIntervalSerializer;

/**
 * The time pools.
 * @param period the period or duration of each pool
 * @param frequency the spacing between pools
 */
@RecordBuilder
@JsonSerialize( using = TimeIntervalSerializer.class )
@JsonDeserialize( using = TimeIntervalDeserializer.class )
public record TimePools( Duration period,
                         Duration frequency )
{
    /**
     * Sets the default values.
     * @param period the period
     * @param frequency the frequency
     */
    public TimePools
    {
        // If the period is non-zero, set the frequency equal to the period by default
        if ( Objects.isNull( frequency ) && !Duration.ZERO.equals( period )  )
        {
            frequency = period;
        }
    }
}
