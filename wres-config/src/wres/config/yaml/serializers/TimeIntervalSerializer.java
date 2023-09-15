package wres.config.yaml.serializers;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.yaml.components.TimePools;

/**
 * Serializes a pair of {@link Duration} that represent an interval.
 * @author James Brown
 */
public class TimeIntervalSerializer extends JsonSerializer<TimePools>
{
    /** The underlying serializer. */
    private static final DurationIntervalSerializer DURATION_INTERVAL_SERIALIZER =
            new DurationIntervalSerializer( "period", "frequency" );

    @Override
    public void serialize( TimePools interval, JsonGenerator gen, SerializerProvider serializers )
            throws IOException
    {
        Objects.requireNonNull( interval );

        Pair<Duration, Duration> durations = Pair.of( interval.period(), interval.frequency() );
        DURATION_INTERVAL_SERIALIZER.serialize( durations, gen, serializers );
    }
}
