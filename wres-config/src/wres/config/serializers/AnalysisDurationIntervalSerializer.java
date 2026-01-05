package wres.config.serializers;

import java.time.Duration;
import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.components.AnalysisTimes;

/**
 * Serializes a pair of {@link Duration} that represent an analysis duration interval.
 * @author James Brown
 */
public class AnalysisDurationIntervalSerializer extends ValueSerializer<AnalysisTimes>
{
    /** The underlying serializer. */
    private static final DurationIntervalSerializer DURATION_INTERVAL_SERIALIZER =
            new DurationIntervalSerializer( "minimum", "maximum" );

    @Override
    public void serialize( AnalysisTimes interval, JsonGenerator gen, SerializationContext serializers )
    {
        Objects.requireNonNull( interval );

        Pair<Duration, Duration> durations = Pair.of( interval.minimum(), interval.maximum() );
        DURATION_INTERVAL_SERIALIZER.serialize( durations, gen, serializers );
    }
}
