package wres.config.serializers;

import java.util.Set;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.FeatureService;
import wres.config.components.FeatureServiceGroup;

/**
 * Serializes a {@link FeatureService}.
 * @author James Brown
 */
public class FeatureServiceSerializer extends ValueSerializer<FeatureService>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureServiceSerializer.class );

    @Override
    public void serialize( FeatureService service, JsonGenerator writer, SerializationContext serializers )
    {
        // Start object
        writer.writeStartObject();

        writer.writeStringProperty( "uri", service.uri()
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
            writer.writeName( "groups" );
            writer.writeStartArray();

            for ( FeatureServiceGroup nextGroup : groups )
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
     */
    private void writeGroup( JsonGenerator writer, FeatureServiceGroup group )
    {
        writer.writeStringProperty( "group", group.group() );
        writer.writeStringProperty( "value", group.value() );
        if ( Boolean.TRUE == group.pool() )
        {
            writer.writeBooleanProperty( "pool", true );
        }
    }

}
