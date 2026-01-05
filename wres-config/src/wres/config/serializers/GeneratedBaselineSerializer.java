package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.DeclarationUtilities;
import wres.config.components.GeneratedBaseline;
import wres.config.components.GeneratedBaselineBuilder;
import wres.config.components.GeneratedBaselines;

/**
 * Serializes a {@link GeneratedBaseline}.
 * @author James Brown
 */
public class GeneratedBaselineSerializer extends ValueSerializer<GeneratedBaseline>
{
    /** Generated baseline with default values that should not be serialized. */
    private static final GeneratedBaseline DEFAULT = GeneratedBaselineBuilder.builder()
                                                                             .build();

    @Override
    public void serialize( GeneratedBaseline generatedBaseline,
                           JsonGenerator writer,
                           SerializationContext serializers )
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
            writer.writeStringProperty( "name", method.toString() );
            writer.writeNumberProperty( "order", generatedBaseline.order() );
            writer.writeEndObject();
        }
        else
        {
            writer.writeStartObject();
            writer.writeStringProperty( "name", method.toString() );
            if ( generatedBaseline.average() != DEFAULT.average() )
            {
                String enumNameString = generatedBaseline.average()
                                                         .toString();
                String enumString = DeclarationUtilities.fromEnumName( enumNameString );
                writer.writeStringProperty( "average", enumString );
            }
            if ( !Objects.equals( generatedBaseline.minimumDate(), DEFAULT.minimumDate() ) )
            {
                writer.writeStringProperty( "minimum_date", generatedBaseline.minimumDate()
                                                                             .toString() );
            }
            if ( !Objects.equals( generatedBaseline.maximumDate(), DEFAULT.maximumDate() ) )
            {
                writer.writeStringProperty( "maximum_date", generatedBaseline.maximumDate()
                                                                             .toString() );
            }
            writer.writeEndObject();
        }
    }
}
