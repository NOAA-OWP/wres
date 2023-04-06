package wres.config.yaml.components;

import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.soabase.recordbuilder.core.RecordBuilder;

import wres.config.yaml.serializers.ChronoUnitSerializer;

/**
 * The time pools.
 * @param period the period or duration of each pool
 * @param frequency the spacing between pools
 * @param unit the time unit
 */
@RecordBuilder
public record TimePools( @JsonProperty( "period" ) Integer period, @JsonProperty( "frequency" ) Integer frequency,
                         @JsonSerialize( using = ChronoUnitSerializer.class )
                         @JsonProperty( "unit" ) ChronoUnit unit )
{
    /**
     * Sets the default values.
     * @param period the period
     * @param frequency the frequency
     * @param unit the unit
     */
    public TimePools
    {
        if ( Objects.isNull( frequency ) && period > 0  )
        {
            frequency = period;
        }
    }
}
