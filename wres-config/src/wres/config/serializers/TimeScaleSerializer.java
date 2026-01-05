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
import wres.config.components.TimeScale;

/**
 * Serializes a {@link TimeScale}.
 * @author James Brown
 */
public class TimeScaleSerializer extends ValueSerializer<TimeScale>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeScaleSerializer.class );

    @Override
    public void serialize( TimeScale value, JsonGenerator gen, SerializationContext serializers )
    {
        LOGGER.debug( "Discovered a timescale to serialize: {}.", value );

        wres.statistics.generated.TimeScale timeScale = value.timeScale();

        // Start
        gen.writeStartObject();

        // Function
        wres.statistics.generated.TimeScale.TimeScaleFunction function = timeScale.getFunction();
        String functionName = DeclarationUtilities.fromEnumName( function.name() );
        gen.writeStringProperty( "function", functionName );

        // Period, if available
        if ( timeScale.getPeriod().getSeconds() > 0 || timeScale.getPeriod().getNanos() > 0 )
        {
            Duration duration = Duration.ofSeconds( timeScale.getPeriod()
                                                             .getSeconds(),
                                                    timeScale.getPeriod()
                                                             .getNanos() );

            Pair<Long, String> serialized = DeclarationUtilities.getDurationInPreferredUnits( duration );

            // Period
            gen.writeNumberProperty( "period", serialized.getLeft() );

            // Units
            gen.writeStringProperty( "unit", serialized.getRight() );
        }

        // Start month
        if ( timeScale.getStartMonth() != 0 )
        {
            gen.writeNumberProperty( "minimum_month", timeScale.getStartMonth() );
        }

        // Start day
        if ( timeScale.getStartDay() != 0 )
        {
            gen.writeNumberProperty( "minimum_day", timeScale.getStartDay() );
        }

        // End month
        if ( timeScale.getEndMonth() != 0 )
        {
            gen.writeNumberProperty( "maximum_month", timeScale.getEndMonth() );
        }

        // End day
        if ( timeScale.getEndDay() != 0 )
        {
            gen.writeNumberProperty( "maximum_day", timeScale.getEndDay() );
        }

        // End
        gen.writeEndObject();
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, TimeScale timeScale )
    {
        return Objects.isNull( timeScale );
    }
}
