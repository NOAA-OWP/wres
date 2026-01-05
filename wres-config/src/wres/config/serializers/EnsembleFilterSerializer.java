package wres.config.serializers;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;

import wres.config.components.EnsembleFilter;

/**
 * Serializes a {@link EnsembleFilter}.
 * @author James Brown
 */
public class EnsembleFilterSerializer extends ValueSerializer<EnsembleFilter>
{
    @Override
    public void serialize( EnsembleFilter filter, JsonGenerator writer, SerializationContext serializers )
    {
        // If the exclude option is not default, write that too
        if ( filter.exclude() )
        {
            writer.writePOJOProperty( "members", filter.members()
                                                       .toArray() );
            writer.writeBooleanProperty( "exclude", true );
        }
        // Otherwise, write a simple array of labels
        else
        {
            writer.writePOJO( filter.members()
                                    .toArray() );
        }
    }
}
