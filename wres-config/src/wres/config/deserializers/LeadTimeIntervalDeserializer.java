package wres.config.deserializers;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

import wres.config.components.LeadTimeInterval;

/**
 * Custom deserializer for a {@link LeadTimeInterval}.
 *
 * @author James Brown
 */
public class LeadTimeIntervalDeserializer extends ValueDeserializer<LeadTimeInterval>
{
    /** The underlying deserializer. */
    private static final DurationIntervalDeserializer DURATION_INTERVAL_DESERIALIZER =
            new DurationIntervalDeserializer( "minimum", "maximum" );

    @Override
    public LeadTimeInterval deserialize( JsonParser jp, DeserializationContext context )
    {
        DurationInterval interval = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );
        return new LeadTimeInterval( interval.left(), interval.right() );
    }
}

