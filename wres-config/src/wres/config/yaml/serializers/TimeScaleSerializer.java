package wres.config.yaml.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.TimeScale;

/**
 * Serializes a {@link TimeScale}.
 * @author James Brown
 */
public class TimeScaleSerializer extends JsonSerializer<TimeScale>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeScaleSerializer.class );

    @Override
    public void serialize( TimeScale value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        wres.statistics.generated.TimeScale timeScale = value.timeScale();

        // Start
        gen.writeStartObject();

        if ( timeScale.getPeriod().getSeconds() > 0 || timeScale.getPeriod().getNanos() > 0 )
        {
            gen.writeFieldName( "period" );
            gen.writeString( String.valueOf( timeScale.getPeriod()
                                                  .getSeconds() ) );

            if( timeScale.getPeriod().getNanos() > 0 )
            {
                LOGGER.warn( "Could not write the nanosecond component of the timescale." );
            }

            gen.writeFieldName( "units" );
            gen.writeString( "seconds" );
        }

        // End
        gen.writeEndObject();
    }

}
