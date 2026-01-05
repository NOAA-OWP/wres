package wres.config.serializers;

import java.time.Duration;
import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationUtilities;

/**
 * Serializes a {@link Duration}.
 * @author James Brown
 */
public class DurationSerializer extends ValueSerializer<Duration>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DurationSerializer.class );

    @Override
    public void serialize( Duration duration, JsonGenerator gen, SerializationContext serializers )
    {
        Objects.requireNonNull( duration );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered a duration of {} to serialize for the {}.",
                          duration,
                          gen.streamWriteContext()
                             .currentName() );
        }

        // Start
        gen.writeStartObject();

        if ( duration.getSeconds() != 0 || duration.getNano() != 0 )
        {
            Pair<Long, String> serialized = DeclarationUtilities.getDurationInPreferredUnits( duration );

            // Period
            gen.writeNumberProperty( "period", serialized.getLeft() );

            // Units
            gen.writeStringProperty( "unit", serialized.getRight() );
        }

        // End
        gen.writeEndObject();
    }
}
