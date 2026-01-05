package wres.config.deserializers;

import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;

import wres.config.components.TimePools;

/**
 * Custom deserializer for a {@link TimePools}.
 *
 * @author James Brown
 */
public class TimePoolsDeserializer extends ValueDeserializer<TimePools>
{
    /** The underlying deserializer. */
    private static final DurationIntervalDeserializer DURATION_INTERVAL_DESERIALIZER =
            new DurationIntervalDeserializer( "period", "frequency" );

    @Override
    public TimePools deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        DurationInterval interval = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );

        return new TimePools( interval.left(),
                              interval.right(),
                              interval.reverse() );
    }
}

