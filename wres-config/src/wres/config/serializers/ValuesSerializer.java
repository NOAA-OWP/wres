package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.Values;

/**
 * Serializes a {@link Values}.
 * @author James Brown
 */
public class ValuesSerializer extends ValueSerializer<Values>
{
    @Override
    public void serialize( Values value, JsonGenerator gen, SerializationContext serializers )
    {
        // Start
        gen.writeStartObject();

        if ( Objects.nonNull( value.minimum() )
             && Double.isFinite( value.minimum() ) )
        {
            gen.writeNumberProperty( "minimum", value.minimum() );
        }

        if ( Objects.nonNull( value.maximum() )
             && Double.isFinite( value.maximum() ) )
        {
            gen.writeNumberProperty( "maximum", value.maximum() );
        }

        if ( Objects.nonNull( value.belowMinimum() )
             && Double.isFinite( value.belowMinimum() ) )
        {
            gen.writeNumberProperty( "below_minimum", value.belowMinimum() );
        }

        if ( Objects.nonNull( value.aboveMaximum() )
             && Double.isFinite( value.aboveMaximum() ) )
        {
            gen.writeNumberProperty( "above_maximum", value.aboveMaximum() );
        }

        // End
        gen.writeEndObject();
    }
}
