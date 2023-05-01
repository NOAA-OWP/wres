package wres.config.yaml.serializers;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.yaml.components.AnalysisDurations;
import wres.config.yaml.components.LeadTimeInterval;

/**
 * Serializes a pair of {@link Duration} that represent an analysis duration interval.
 * @author James Brown
 */
public class AnalysisDurationIntervalSerializer extends JsonSerializer<AnalysisDurations>
{
    /** The underlying serializer. */
    private static final DurationIntervalSerializer DURATION_INTERVAL_SERIALIZER =
            new DurationIntervalSerializer( "minimum_exclusive", "maximum" );

    @Override
    public void serialize( AnalysisDurations interval, JsonGenerator gen, SerializerProvider serializers )
            throws IOException
    {
        Objects.requireNonNull( interval );

        Pair<Duration, Duration> durations = Pair.of( interval.minimumExclusive(), interval.maximum() );
        DURATION_INTERVAL_SERIALIZER.serialize( durations, gen, serializers );
    }
}
