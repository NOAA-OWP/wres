package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;

/**
 * Serializes a {@link GeneratedBaseline}.
 * @author James Brown
 */
public class GeneratedBaselineSerializer extends JsonSerializer<GeneratedBaseline>
{
    /** Generated baseline with default values that should not be serialized. */
    private static final GeneratedBaseline DEFAULT = GeneratedBaselineBuilder.builder()
                                                                             .build();

    @Override
    public void serialize( GeneratedBaseline generatedBaseline,
                           JsonGenerator writer,
                           SerializerProvider serializers ) throws IOException
    {
        GeneratedBaselines method = generatedBaseline.method();
        // All parameters are default, write short form
        GeneratedBaseline withMethod = GeneratedBaselineBuilder.builder( DEFAULT )
                                                               .method( method )
                                                               .build();
        if ( withMethod.equals( generatedBaseline ) )
        {
            writer.writeString( method.toString() );
        }
        else if ( method == GeneratedBaselines.PERSISTENCE
                  && !Objects.equals( generatedBaseline.order(), DEFAULT.order() ) )
        {
            writer.writeStartObject();
            writer.writeStringField( "name", method.toString() );
            writer.writeNumberField( "order", generatedBaseline.order() );
            writer.writeEndObject();
        }
        else
        {
            writer.writeStartObject();
            writer.writeStringField( "name", method.toString() );
            if ( generatedBaseline.average() != DEFAULT.average() )
            {
                String enumNameString = generatedBaseline.average()
                                                   .toString();
                String enumString = DeclarationUtilities.fromEnumName( enumNameString );
                writer.writeStringField( "average", enumString );
            }
            if ( !Objects.equals( generatedBaseline.minimumDate(), DEFAULT.minimumDate() ) )
            {
                writer.writeStringField( "minimum_date", generatedBaseline.minimumDate()
                                                                          .toString() );
            }
            if ( !Objects.equals( generatedBaseline.maximumDate(), DEFAULT.maximumDate() ) )
            {
                writer.writeStringField( "maximum_date", generatedBaseline.maximumDate()
                                                                          .toString() );
            }
            writer.writeEndObject();
        }
    }
}
