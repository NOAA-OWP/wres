package wres.config.serializers;

import java.util.Objects;
import java.util.Set;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.Features;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Geometry;

/**
 * Serializes a {@link Features}.
 * @author James Brown
 */
public class FeaturesSerializer extends ValueSerializer<Features>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeaturesSerializer.class );

    @Override
    public void serialize( Features features, JsonGenerator gen, SerializationContext serializers )
    {
        Set<GeometryTuple> geometries = features.geometries();
        this.serialize( geometries, gen );
    }

    @Override
    public boolean isEmpty( SerializationContext serializers, Features features )
    {
        return Objects.isNull( features ) || features.geometries()
                                                     .isEmpty();
    }

    /**
     * Serialize the geometries.
     * @param geometries the geometries
     * @param writer the writer
     */
    void serialize( Set<GeometryTuple> geometries, JsonGenerator writer )
    {
        if ( !geometries.isEmpty() )
        {
            LOGGER.debug( "Discovered a collection of geometries with {} members.", geometries.size() );

            writer.writeStartArray();

            // Write each geometry
            for ( GeometryTuple next : geometries )
            {
                this.writeGeometryTuple( next, writer );
            }

            // End
            writer.writeEndArray();
        }
    }

    /**
     * Writes a {@link GeometryTuple}.
     * @param geometryTuple the geometry tuple
     * @param writer the writer
     */
    private void writeGeometryTuple( GeometryTuple geometryTuple, JsonGenerator writer )
    {
        writer.writeStartObject();
        if ( geometryTuple.hasLeft() )
        {
            this.writeGeometry( geometryTuple.getLeft(), writer, "observed" );
        }
        if ( geometryTuple.hasRight() )
        {
            this.writeGeometry( geometryTuple.getRight(), writer, "predicted" );
        }
        if ( geometryTuple.hasBaseline() )
        {
            this.writeGeometry( geometryTuple.getBaseline(), writer, "baseline" );
        }
        writer.writeEndObject();
    }

    /**
     * Writes a {@link Geometry}.
     * @param geometry the geometry
     * @param writer the writer
     * @param context the context for the geometry
     */
    private void writeGeometry( Geometry geometry, JsonGenerator writer, String context )
    {
        if ( this.isSimpleGeometry( geometry ) )
        {
            writer.writeName( context );
            writer.writeString( geometry.getName() );
        }
        else
        {
            writer.writeName( context );
            writer.writeStartObject();
            writer.writeStringProperty( "name", geometry.getName() );

            if ( !geometry.getDescription()
                          .isBlank() )
            {
                writer.writeStringProperty( "description", geometry.getDescription() );
            }
            if ( !geometry.getWkt()
                          .isBlank() )
            {
                writer.writeStringProperty( "wkt", geometry.getWkt() );
            }
            if ( geometry.getSrid() != 0 )
            {
                writer.writeNumberProperty( "srid", geometry.getSrid() );
            }
            writer.writeEndObject();
        }
    }

    /**
     * @param geometry geometry
     * @return whether the geometry is simple
     */
    private boolean isSimpleGeometry( Geometry geometry )
    {
        // Only the name is defined: this approach should be robust to extension of the geometry in future
        return Geometry.newBuilder()
                       .setName( geometry.getName() )
                       .build()
                       .equals( geometry );
    }

}