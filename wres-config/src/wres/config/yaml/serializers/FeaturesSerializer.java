package wres.config.yaml.serializers;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.components.Features;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Geometry;

/**
 * Serializes a {@link Features}.
 * @author James Brown
 */
public class FeaturesSerializer extends JsonSerializer<Features>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( FeaturesSerializer.class );

    @Override
    public void serialize( Features features, JsonGenerator gen, SerializerProvider serializers ) throws IOException
    {
        Set<GeometryTuple> geometries = features.geometries();
        this.serialize( geometries, gen );
    }

    @Override
    public boolean isEmpty( SerializerProvider serializers, Features features )
    {
        return Objects.isNull( features ) || features.geometries()
                                                     .isEmpty();
    }

    /**
     * Serialize the geometries.
     * @param geometries the geometries
     * @param writer the writer
     * @throws IOException if the geometries could not be written for any reason
     */
    void serialize( Set<GeometryTuple> geometries, JsonGenerator writer ) throws IOException
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
     * @throws IOException if the geometry tuple could not be written for any reason
     */
    private void writeGeometryTuple( GeometryTuple geometryTuple, JsonGenerator writer ) throws IOException
    {
        // Use flow style if possible
        if( writer instanceof CustomGenerator custom )
        {
            custom.setFlowStyleOn();
        }

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

        // Return to default style
        if( writer instanceof CustomGenerator custom )
        {
            custom.setFlowStyleOff();
        }
    }

    /**
     * Writes a {@link Geometry}.
     * @param geometry the geometry
     * @param writer the writer
     * @param context the context for the geometry
     * @throws IOException if the geometry could not be written for any reason
     */
    private void writeGeometry( Geometry geometry, JsonGenerator writer, String context ) throws IOException
    {
        if ( this.isSimpleGeometry( geometry ) )
        {
            writer.writeFieldName( context );
            writer.writeString( geometry.getName() );
        }
        else
        {
            writer.writeFieldName( context );
            writer.writeStartObject();
            writer.writeStringField( "name", geometry.getName() );

            if ( !geometry.getDescription()
                          .isBlank() )
            {
                writer.writeStringField( "description", geometry.getDescription() );
            }
            if ( !geometry.getWkt()
                          .isBlank() )
            {
                writer.writeStringField( "wkt", geometry.getWkt() );
            }
            if ( geometry.getSrid() != 0 )
            {
                writer.writeFieldName( "srid" );
                writer.writeNumber( geometry.getSrid() );
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