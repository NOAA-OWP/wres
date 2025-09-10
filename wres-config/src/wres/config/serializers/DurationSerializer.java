package wres.config.serializers;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationUtilities;

/**
 * Serializes a {@link Duration}.
 * @author James Brown
 */
public class DurationSerializer extends JsonSerializer<Duration>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DurationSerializer.class );

    @Override
    public void serialize( Duration duration, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        Objects.requireNonNull( duration );

        LOGGER.debug( "Discovered a duration of {} to serialize for the {}.", duration, gen.getOutputContext()
                                                                                           .getCurrentName() );

        // Start
        gen.writeStartObject();

        if ( duration.getSeconds() != 0 || duration.getNano() != 0 )
        {
            Pair<Long,String> serialized = DeclarationUtilities.getDurationInPreferredUnits( duration );

            // Period
            gen.writeNumberField( "period", serialized.getLeft() );

            // Units
            gen.writeStringField( "unit", serialized.getRight() );
        }

        // End
        gen.writeEndObject();
    }
}
