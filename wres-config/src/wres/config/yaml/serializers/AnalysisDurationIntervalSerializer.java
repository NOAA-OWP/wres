package wres.config.yaml.serializers;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.yaml.components.AnalysisTimes;

/**
 * Serializes a pair of {@link Duration} that represent an analysis duration interval.
 * @author James Brown
 */
public class AnalysisDurationIntervalSerializer extends JsonSerializer<AnalysisTimes>
{
    /** The underlying serializer. */
    private static final DurationIntervalSerializer DURATION_INTERVAL_SERIALIZER =
            new DurationIntervalSerializer( "minimum", "maximum" );

    @Override
    public void serialize( AnalysisTimes interval, JsonGenerator gen, SerializerProvider serializers )
            throws IOException
    {
        Objects.requireNonNull( interval );

        Pair<Duration, Duration> durations = Pair.of( interval.minimum(), interval.maximum() );
        DURATION_INTERVAL_SERIALIZER.serialize( durations, gen, serializers );
    }
}
