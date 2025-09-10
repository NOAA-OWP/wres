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
import wres.config.components.TimeScale;

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
        LOGGER.debug( "Discovered a timescale to serialize: {}.", value );

        wres.statistics.generated.TimeScale timeScale = value.timeScale();

        // Start
        gen.writeStartObject();

        // Function
        wres.statistics.generated.TimeScale.TimeScaleFunction function = timeScale.getFunction();
        String functionName = DeclarationUtilities.fromEnumName( function.name() );
        gen.writeStringField( "function", functionName );

        // Period, if available
        if ( timeScale.getPeriod().getSeconds() > 0 || timeScale.getPeriod().getNanos() > 0 )
        {
            Duration duration = Duration.ofSeconds( timeScale.getPeriod()
                                                             .getSeconds(),
                                                    timeScale.getPeriod()
                                                             .getNanos() );

            Pair<Long,String> serialized = DeclarationUtilities.getDurationInPreferredUnits( duration );

            // Period
            gen.writeNumberField( "period", serialized.getLeft() );

            // Units
            gen.writeStringField( "unit", serialized.getRight() );
        }

        // Start month
        if ( timeScale.getStartMonth() != 0 )
        {
            gen.writeNumberField( "minimum_month", timeScale.getStartMonth() );
        }

        // Start day
        if ( timeScale.getStartDay() != 0 )
        {
            gen.writeNumberField( "minimum_day", timeScale.getStartDay() );
        }

        // End month
        if ( timeScale.getEndMonth() != 0 )
        {
            gen.writeNumberField( "maximum_month", timeScale.getEndMonth() );
        }

        // End day
        if ( timeScale.getEndDay() != 0 )
        {
            gen.writeNumberField( "maximum_day", timeScale.getEndDay() );
        }

        // End
        gen.writeEndObject();
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, TimeScale timeScale )
    {
        return Objects.isNull( timeScale );
    }
}
