package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.FeatureGroups;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Serializes a {@link FeatureGroups}.
 * @author James Brown
 */
public class FeatureGroupsSerializer extends JsonSerializer<FeatureGroups>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureGroupsSerializer.class );

    private static final FeaturesSerializer FEATURES_SERIALIZER = new FeaturesSerializer();

    @Override
    public void serialize( FeatureGroups value, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        Set<GeometryGroup> groups = value.geometryGroups();

        if ( !groups.isEmpty() )
        {
            LOGGER.debug( "Discovered a collection of geometry groups with {} members.", groups.size() );

            // Write each group
            gen.writeStartArray();
            for ( GeometryGroup next : groups )
            {
                this.writeGeometryGroup( next, gen );
            }
            gen.writeEndArray();
        }
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, FeatureGroups value )
    {
        return Objects.isNull( value ) || value.geometryGroups()
                                               .isEmpty();
    }

    /**
     * Writes a {@link GeometryGroup}.
     * @param group the geometry group
     * @param writer the writer
     * @throws IOException if the geometry group could not be written for any reason
     */
    private void writeGeometryGroup( GeometryGroup group,
                                     JsonGenerator writer ) throws IOException
    {
        if ( !group.getRegionName()
                   .isBlank() )
        {
            writer.writeStartObject();
            writer.writeStringField( "name", group.getRegionName() );

            // Preserve insertion order
            Set<GeometryTuple> geometries = new LinkedHashSet<>( group.getGeometryTuplesList() );
            writer.writeFieldName( "features" );
            FEATURES_SERIALIZER.serialize( geometries,
                                           writer );
            writer.writeEndObject();
        }
    }
}
