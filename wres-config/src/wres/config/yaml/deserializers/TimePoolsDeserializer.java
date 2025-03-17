package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import wres.config.yaml.components.TimePools;

/**
 * Custom deserializer for a {@link TimePools}.
 *
 * @author James Brown
 */
public class TimePoolsDeserializer extends JsonDeserializer<TimePools>
{
    /** The underlying deserializer. */
    private static final DurationIntervalDeserializer DURATION_INTERVAL_DESERIALIZER =
            new DurationIntervalDeserializer( "period", "frequency" );

    @Override
    public TimePools deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        DurationInterval interval = DURATION_INTERVAL_DESERIALIZER.deserialize( jp, context );

        return new TimePools( interval.left(),
                              interval.right(),
                              interval.reverse() );
    }
}

