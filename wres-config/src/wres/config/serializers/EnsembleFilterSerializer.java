package wres.config.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import wres.config.components.EnsembleFilter;

/**
 * Serializes a {@link EnsembleFilter}.
 * @author James Brown
 */
public class EnsembleFilterSerializer extends JsonSerializer<EnsembleFilter>
{
    @Override
    public void serialize( EnsembleFilter filter, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        // If the exclude option is not default, write that too
        if ( filter.exclude() )
        {
            writer.writeObjectField( "members", filter.members()
                                                      .toArray() );
            writer.writeBooleanField( "exclude", true );
        }
        // Otherwise, write a simple array of labels
        else
        {
            writer.writeObject( filter.members()
                                      .toArray() );
        }
    }
}
