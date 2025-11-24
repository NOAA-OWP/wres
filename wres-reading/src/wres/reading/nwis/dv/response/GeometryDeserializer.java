package wres.reading.nwis.dv.response;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wololo.jts2geojson.GeoJSONReader;

/**
 * Custom deserializer for a {@link org.locationtech.jts.geom.Geometry}.
 *
 * @author James Brown
 */
public class GeometryDeserializer extends JsonDeserializer<Geometry>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( GeometryDeserializer.class );

    /** Cache of geometries for re-use. */
    private static final Cache<Geometry, Geometry> GEOMETRY_CACHE =
            Caffeine.newBuilder()
                    .maximumSize( 100 ) // 100, arbitrarily
                    .build();

    @Override
    public Geometry deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectMapper mapper = ( ObjectMapper ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );
        String geoString = mapper.writeValueAsString( node );

        LOGGER.debug( "Discovered this GeoJSON string to parse: {}", geoString );

        GeoJSONReader reader = new GeoJSONReader();

        Geometry geometry = reader.read( geoString );

        Geometry cachedKey = GEOMETRY_CACHE.getIfPresent( geometry );

        if ( Objects.nonNull( cachedKey ) )
        {
            LOGGER.debug( "Returning geometry from cache: {}", cachedKey );
            return cachedKey;
        }
        else
        {
            GEOMETRY_CACHE.put( geometry, geometry );
            return geometry;
        }
    }
}

