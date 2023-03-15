package wres.config.yaml.components;

import java.time.temporal.ChronoUnit;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.soabase.recordbuilder.core.RecordBuilder;

/**
 * The time pools.
 * @param period the period or duration of each pool
 * @param frequency the spacing between pools
 * @param unit the time unit
 */
@RecordBuilder
public record TimePools( @JsonProperty( "period" ) Integer period, @JsonProperty( "frequency" ) Integer frequency,
                         @JsonProperty( "unit" ) ChronoUnit unit )
{
    // Set defaults
    public TimePools
    {
        if ( Objects.isNull( frequency ) )
        {
            frequency = period;
        }
    }
}
