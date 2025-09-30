package wres.config.deserializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import wres.config.components.LeadTimeInterval;

/**
 * Custom deserializer for a {@link LeadTimeInterval}.
 *
 * @author James Brown
 */
public class LeadTimeIntervalDeserializer extends JsonDeserializer<LeadTimeInterval>
{
    /** The underlying deserializer. */
    private static final DurationIntervalDeserializer DURATION_INTERVAL_DESERIALIZER =
            new DurationIntervalDeserializer( "minimum", "maximum" );

    @Override
    public LeadTimeInterval deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        DurationInterval interval = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );
        return new LeadTimeInterval( interval.left(), interval.right() );
    }
}

