package wres.config.serializers;

import java.time.Duration;
import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.components.TimePools;

/**
 * Serializes a pair of {@link Duration} that represent an interval.
 * @author James Brown
 */
public class TimeIntervalSerializer extends ValueSerializer<TimePools>
{
    /** The underlying serializer. */
    private static final DurationIntervalSerializer DURATION_INTERVAL_SERIALIZER =
            new DurationIntervalSerializer( "period", "frequency" );

    @Override
    public void serialize( TimePools interval, JsonGenerator gen, SerializationContext serializers )
    {
        Objects.requireNonNull( interval );

        Pair<Duration, Duration> durations = Pair.of( interval.period(), interval.frequency() );
        DURATION_INTERVAL_SERIALIZER.serialize( durations, gen, serializers );
    }
}
