package wres.reading.nwis.ogc.response;

import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.jspecify.annotations.NonNull;

/**
 * Custom deserializer for a {@link Properties}. Has a reduced footprint when many similar instances are constructed.
 * This is achieved by caching and re-using strings that are repeated across many instances.
 *
 * @author James Brown
 */
public class PropertiesDeserializer extends ValueDeserializer<Properties>
{
    /** Cache of geometries for re-use. */
    private static final Cache<@NonNull String, String> STRING_CACHE =
            Caffeine.newBuilder()
                    .maximumSize( 100 ) // 100, arbitrarily
                    .build();

    @Override
    public Properties deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext reader = jp.objectReadContext();
        JsonNode node = reader.readTree( jp );

        String unit = this.getPropertyFromJsonOrCache( node, "unit_of_measure" );
        String statisticId = this.getPropertyFromJsonOrCache( node, "statistic_id" );
        String locationId = this.getPropertyFromJsonOrCache( node, "monitoring_location_id" );
        String timeSeriesId = this.getPropertyFromJsonOrCache( node, "time_series_id" );

        String time = node.get( "time" )
                          .asString();

        double value = node.get( "value" )
                           .asDouble();

        return Properties.builder()
                         .locationId( locationId )
                         .statistic( statisticId )
                         .unit( unit )
                         .timeSeriesId( timeSeriesId )
                         .time( time )
                         .value( value )
                         .build();
    }

    /**
     * Returns a property from the supplied node or from the cache if it has been read before.
     * @param node the node
     * @param propertyName the property name
     * @return the new property or cached property
     */

    private String getPropertyFromJsonOrCache( JsonNode node, String propertyName )
    {
        String property = null;

        if ( node.has( propertyName ) )
        {
            String innerproperty = node.get( propertyName )
                                       .asString();

            // Now create a new one with cached content, where possible
            String cachedProperty = STRING_CACHE.getIfPresent( innerproperty );

            if ( Objects.isNull( cachedProperty ) )
            {
                STRING_CACHE.put( innerproperty, innerproperty );
            }
            else
            {
                innerproperty = cachedProperty;
            }

            property = innerproperty;
        }

        return property;
    }
}

