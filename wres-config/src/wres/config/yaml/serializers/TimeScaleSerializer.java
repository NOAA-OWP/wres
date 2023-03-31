package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.FeatureGroups;
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

        // Function
        wres.statistics.generated.TimeScale.TimeScaleFunction function = timeScale.getFunction();
        String functionName = DeclarationFactory.getFriendlyName( function.name() );
        gen.writeStringField( "function", functionName );

        // Period, if available
        if ( timeScale.getPeriod().getSeconds() > 0 || timeScale.getPeriod().getNanos() > 0 )
        {
            // Period
            gen.writeStringField( "period", String.valueOf( timeScale.getPeriod()
                                                                     .getSeconds() ) );

            if ( timeScale.getPeriod().getNanos() > 0 )
            {
                LOGGER.warn( "Could not write the nanosecond component of the timescale." );
            }

            // Units
            gen.writeStringField( "unit", "seconds" );
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
