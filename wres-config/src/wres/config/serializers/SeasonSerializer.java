package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.Season;

/**
 * Serializes a {@link Season}.
 * @author James Brown
 */
public class SeasonSerializer extends ValueSerializer<Season>
{
    @Override
    public void serialize( Season value, JsonGenerator gen, SerializationContext serializers )
    {
        // Start
        gen.writeStartObject();

        if ( Objects.nonNull( value.minimum() ) )
        {
            gen.writeNumberProperty( "minimum_day", value.minimum()
                                                         .getDayOfMonth() );
            gen.writeNumberProperty( "minimum_month", value.minimum()
                                                           .getMonthValue() );
        }
        if ( Objects.nonNull( value.maximum() ) )
        {
            gen.writeNumberProperty( "maximum_day", value.maximum()
                                                         .getDayOfMonth() );
            gen.writeNumberProperty( "maximum_month", value.maximum()
                                                           .getMonthValue() );
        }

        // End
        gen.writeEndObject();
    }
}
