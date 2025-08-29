package wres.config.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.components.Values;

/**
 * Serializes a {@link Values}.
 * @author James Brown
 */
public class ValuesSerializer extends JsonSerializer<Values>
{
    @Override
    public void serialize( Values value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        // Start
        gen.writeStartObject();

        if ( Objects.nonNull( value.minimum() )
             && Double.isFinite( value.minimum() ) )
        {
            gen.writeFieldName( "minimum" );
            gen.writeNumber( value.minimum() );
        }

        if ( Objects.nonNull( value.maximum() )
             && Double.isFinite( value.maximum() ) )
        {
            gen.writeFieldName( "maximum" );
            gen.writeNumber( value.maximum() );
        }

        if ( Objects.nonNull( value.belowMinimum() )
             && Double.isFinite( value.belowMinimum() ) )
        {
            gen.writeFieldName( "below_minimum" );
            gen.writeNumber( value.belowMinimum() );
        }

        if ( Objects.nonNull( value.aboveMaximum() )
             && Double.isFinite( value.aboveMaximum() ) )
        {
            gen.writeFieldName( "above_maximum" );
            gen.writeNumber( value.aboveMaximum() );
        }

        // End
        gen.writeEndObject();
    }
}
