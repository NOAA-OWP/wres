package wres.config.yaml.deserializers;

import java.io.IOException;
import java.time.Duration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.yaml.components.AnalysisTimes;

/**
 * Custom deserializer for a {@link AnalysisTimes}.
 *
 * @author James Brown
 */
public class AnalysisDurationIntervalDeserializer extends JsonDeserializer<AnalysisTimes>
{
    /** The underlying deserializer. */
    private static final DurationIntervalDeserializer DURATION_INTERVAL_DESERIALIZER =
            new DurationIntervalDeserializer( "minimum", "maximum" );

    @Override
    public AnalysisTimes deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Pair<Duration, Duration> pair = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );
        return new AnalysisTimes( pair.getLeft(), pair.getRight() );
    }
}

