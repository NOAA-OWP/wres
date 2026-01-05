package wres.config.serializers;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.FeatureGroups;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Serializes a {@link FeatureGroups}.
 * @author James Brown
 */
public class FeatureGroupsSerializer extends ValueSerializer<FeatureGroups>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureGroupsSerializer.class );

    private static final FeaturesSerializer FEATURES_SERIALIZER = new FeaturesSerializer();

    @Override
    public void serialize( FeatureGroups value, JsonGenerator gen, SerializationContext serializers )
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
    public boolean isEmpty( SerializationContext serializers, FeatureGroups value )
    {
        return Objects.isNull( value ) || value.geometryGroups()
                                               .isEmpty();
    }

    /**
     * Writes a {@link GeometryGroup}.
     * @param group the geometry group
     * @param writer the writer
     */
    private void writeGeometryGroup( GeometryGroup group,
                                     JsonGenerator writer )
    {
        writer.writeStartObject();
        if ( !group.getRegionName()
                   .isBlank() )
        {
            writer.writeStringProperty( "name", group.getRegionName() );
        }

        // Preserve insertion order
        Set<GeometryTuple> geometries = new LinkedHashSet<>( group.getGeometryTuplesList() );
        if ( !geometries.isEmpty() )
        {
            writer.writeName( "features" );
            FEATURES_SERIALIZER.serialize( geometries,
                                           writer );
        }
        writer.writeEndObject();
    }
}
