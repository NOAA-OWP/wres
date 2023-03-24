package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.components.Season;

/**
 * Serializes a {@link Season}.
 * @author James Brown
 */
public class SeasonSerializer extends JsonSerializer<Season>
{
    @Override
    public void serialize( Season value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        // Start
        gen.writeStartObject();

        if ( Objects.nonNull( value.minimum() ) )
        {
            gen.writeFieldName( "minimum_day" );
            gen.writeNumber( value.minimum()
                                  .getDayOfMonth() );
            gen.writeFieldName( "minimum_month" );
            gen.writeNumber( value.minimum()
                                  .getMonthValue() );
        }
        if ( Objects.nonNull( value.maximum() ) )
        {
            gen.writeFieldName( "maximum_day" );
            gen.writeNumber( value.maximum()
                                  .getDayOfMonth() );
            gen.writeFieldName( "maximum_month" );
            gen.writeNumber( value.maximum()
                                  .getMonthValue() );
        }

        // End
        gen.writeEndObject();
    }
}
