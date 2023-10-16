package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;

import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;

/**
 * Serializes a {@link Source}.
 * @author James Brown
 */
public class SourceSerializer extends JsonSerializer<Source>
{
    @Override
    public void serialize( Source source, JsonGenerator writer, SerializerProvider serializers ) throws IOException
    {
        // Skip the URI if that is the only attribute present
        Source compare = SourceBuilder.builder()
                                      .uri( source.uri() )
                                      .build();
        // URI only
        if ( compare.equals( source )
             && Objects.nonNull( source.uri() ) )
        {
            writer.writeString( source.uri()
                                      .toString() );
        }
        // Full description
        else
        {
            // Some shenanigans to get a default serializer that is not this instance and hence not infinitely recursive
            // There should be a simpler way to call a default serializer from a custom serializer. Yuck.
            JavaType javaType = serializers.constructType( Source.class );
            BeanDescription beanDesc = serializers.getConfig()
                                                  .introspect( javaType );
            JsonSerializer<Object> serializer
                    = BeanSerializerFactory.instance.findBeanOrAddOnSerializer( serializers,
                                                                                javaType,
                                                                                beanDesc,
                                                                                false );
            serializer.serialize( source, writer, serializers );
        }
    }
}
