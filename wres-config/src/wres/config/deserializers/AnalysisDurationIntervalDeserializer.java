package wres.config.deserializers;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

import wres.config.components.AnalysisTimes;

/**
 * Custom deserializer for a {@link AnalysisTimes}.
 *
 * @author James Brown
 */
public class AnalysisDurationIntervalDeserializer extends ValueDeserializer<AnalysisTimes>
{
    /** The underlying deserializer. */
    private static final DurationIntervalDeserializer DURATION_INTERVAL_DESERIALIZER =
            new DurationIntervalDeserializer( "minimum", "maximum" );

    @Override
    public AnalysisTimes deserialize( JsonParser jp, DeserializationContext context )
    {
        DurationInterval interval = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );
        return new AnalysisTimes( interval.left(), interval.right() );
    }
}

