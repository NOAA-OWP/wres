package wres.config.serializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;

import wres.config.components.SpatialMask;

/**
 * Serializes a {@link SpatialMask}.
 * @author James Brown
 */
public class SpatialMaskSerializer extends JsonSerializer<SpatialMask>
{
    /** Converts from geometries to WKT strings. */
    private static final WKTWriter WKT_WRITER = new WKTWriter();

    @Override
    public void serialize( SpatialMask spatialMask,
                           JsonGenerator gen,
                           SerializerProvider serializers ) throws IOException
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
            if( Objects.nonNull( name ) && ! name.isBlank() )
            {
                gen.writeStringField( "name", name );
            }

            gen.writeStringField( "wkt", wktString );

            if( srid != 0 )
            {
                gen.writeNumberField( "srid", srid );
            }
        }
        // WKT string only
        else
        {
            gen.writeStringField( "spatial_mask", wktString );
        }

        // End
        gen.writeEndObject();
    }
}
