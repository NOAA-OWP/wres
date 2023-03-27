package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceGroup;

/**
 * Serializes a {@link FeatureService}.
 * @author James Brown
 */
public class FeatureServiceSerializer extends JsonSerializer<FeatureService>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureServiceSerializer.class );

    @Override
    public void serialize( FeatureService service, JsonGenerator writer, SerializerProvider serializers )
            throws IOException
    {
        // Start object
        writer.writeStartObject();

        writer.writeStringField( "uri", service.uri()
                                               .toString() );
        Set<FeatureServiceGroup> groups = service.featureGroups();

        LOGGER.debug( "Discovered a feature service with {} feature group to serialize.", groups.size() );

        if ( groups.size() == 1 )
        {
            FeatureServiceGroup singleton = groups.iterator()
                                                  .next();
            this.writeGroup( writer, singleton );
        }
        else if ( groups.size() > 1 )
        {
            writer.writeFieldName( "groups" );
            writer.writeStartArray();

            for( FeatureServiceGroup nextGroup : groups )
            {
                writer.writeStartObject();
                this.writeGroup( writer, nextGroup );
                writer.writeEndObject();
            }

            writer.writeEndArray();
        }

        // End object
        writer.writeEndObject();
    }

    /**
     * @param writer the writer
     * @param group the group to write
     * @throws IOException if the group could not be written for any reason
     */
    private void writeGroup( JsonGenerator writer, FeatureServiceGroup group ) throws IOException
    {
        writer.writeStringField( "group", group.group() );
        writer.writeStringField( "value", group.value() );
        if ( Boolean.TRUE == group.pool() )
        {
            writer.writeBooleanField( "pool", true );
        }
    }

}
