package wres.config.serializers;

import java.util.Objects;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.SerializationContext;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

import wres.config.components.SpatialMask;

/**
 * Serializes a {@link SpatialMask}.
 * @author James Brown
 */
public class SpatialMaskSerializer extends ValueSerializer<SpatialMask>
{
    /** Converts from geometries to WKT strings. */
    private static final WKTWriter WKT_WRITER = new WKTWriter();

    @Override
    public void serialize( SpatialMask spatialMask,
                           JsonGenerator gen,
                           SerializationContext serializers )
    {
        // Start
        gen.writeStartObject();

        Geometry geometry = spatialMask.geometry();

        // Create the wkt string
        String wktString = WKT_WRITER.write( geometry );

        String name = spatialMask.name();
        int srid = geometry.getSRID();

        // Full description
        if ( Objects.nonNull( name ) || srid != 0 )
        {
            if ( Objects.nonNull( name ) && !name.isBlank() )
            {
                gen.writeStringProperty( "name", name );
            }

            gen.writeStringProperty( "wkt", wktString );

            if ( srid != 0 )
            {
                gen.writeNumberProperty( "srid", srid );
            }
        }
        // WKT string only
        else
        {
            gen.writeStringProperty( "spatial_mask", wktString );
        }

        // End
        gen.writeEndObject();
    }
}
