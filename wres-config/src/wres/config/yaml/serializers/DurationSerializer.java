package wres.config.yaml.serializers;

import java.io.IOException;
import java.time.Duration;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes a {@link Duration}.
 * @author James Brown
 */
public class DurationSerializer extends JsonSerializer<Duration>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DurationSerializer.class );

    @Override
    public void serialize( Duration value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        // Start
        gen.writeStartObject();

        if ( value.getSeconds() != 0 || value.getNano() != 0 )
        {
            gen.writeFieldName( "period" );
            gen.writeString( String.valueOf( value.getSeconds() ) );

            if( value.getNano() != 0 )
            {
                LOGGER.warn( "Could not write the nanosecond component of the duration." );
            }

            gen.writeFieldName( "unit" );
            gen.writeString( "seconds" );
        }

        // End
        gen.writeEndObject();
    }
}
