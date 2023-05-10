package wres.config.yaml.deserializers;

import java.io.IOException;
import java.time.Duration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.yaml.components.AnalysisDurations;

/**
 * Custom deserializer for a {@link AnalysisDurations}.
 *
 * @author James Brown
 */
public class AnalysisDurationIntervalDeserializer extends JsonDeserializer<AnalysisDurations>
{
    /** The underlying deserializer. */
    private static final DurationIntervalDeserializer DURATION_INTERVAL_DESERIALIZER =
            new DurationIntervalDeserializer( "minimum_exclusive", "maximum" );

    @Override
    public AnalysisDurations deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Pair<Duration, Duration> pair = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );
        return new AnalysisDurations( pair.getLeft(), pair.getRight() );
    }
}

