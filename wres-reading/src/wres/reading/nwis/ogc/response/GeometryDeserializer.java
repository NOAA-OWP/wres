package wres.reading.nwis.ogc.response;

import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.NonNull;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wololo.jts2geojson.GeoJSONReader;
import tools.jackson.databind.ValueDeserializer;

/**
 * Custom deserializer for a {@link org.locationtech.jts.geom.Geometry}.
 *
 * @author James Brown
 */
public class GeometryDeserializer extends ValueDeserializer<Geometry>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( GeometryDeserializer.class );

    /** Cache of geometries for re-use. */
    private static final Cache<@NonNull Geometry, Geometry> GEOMETRY_CACHE =
            Caffeine.newBuilder()
                    .maximumSize( 100 ) // 100, arbitrarily
                    .build();

    @Override
    public Geometry deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext mapper = jp.objectReadContext();
        JsonNode node = mapper.readTree( jp );
        String geoString = node.toString();

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

