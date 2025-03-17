package wres.config.yaml.deserializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

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
        DurationInterval interval = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );
        return new AnalysisTimes( interval.left(), interval.right() );
    }
}

