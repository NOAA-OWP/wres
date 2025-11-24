package wres.reading.nwis.dv.response;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Custom deserializer for a {@link Properties}. Has a reduced footprint when many similar instances are constructed.
 * This is achieved by caching and re-using strings that are repeated across many instances.
 *
 * @author James Brown
 */
public class PropertiesDeserializer extends JsonDeserializer<Properties>
{
    /** Cache of geometries for re-use. */
    private static final Cache<String, String> STRING_CACHE =
            Caffeine.newBuilder()
                    .maximumSize( 100 ) // 100, arbitrarily
                    .build();

    @Override
    public Properties deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectCodec oc = jp.getCodec();
        JsonNode node = oc.readTree( jp );

        // Shenanigans to obtain a default deserializer to deserialize the contents first before caching re-usable
        // content
        // https://stackoverflow.com/questions/18313323/how-do-i-call-the-default-deserializer-from-a-custom-deserializer-in-jackson
        DeserializationConfig config = context.getConfig();
        JavaType type = TypeFactory.defaultInstance().constructType( Properties.class );
        JsonDeserializer<Object> defaultDeserializer =
                BeanDeserializerFactory.instance.buildBeanDeserializer( context, type, config.introspect( type ) );

        if ( defaultDeserializer instanceof ResolvableDeserializer resolvableDeserializer )
        {
            resolvableDeserializer.resolve( context );
        }

        JsonParser treeParser = oc.treeAsTokens( node );
        config.initialize( treeParser );

        if ( treeParser.getCurrentToken() == null )
        {
            treeParser.nextToken();
        }

        // Deserialize using the default deserializer
        Properties properties = ( Properties ) defaultDeserializer.deserialize( treeParser, context );

        // Now create a new one with cached content, where possible
        String parameterCode = STRING_CACHE.getIfPresent( properties.getParameterCode() );

        if ( Objects.isNull( parameterCode ) )
        {
            parameterCode = properties.getParameterCode();
            STRING_CACHE.put( parameterCode, parameterCode );
        }

        String unit = STRING_CACHE.getIfPresent( properties.getUnit() );

        if ( Objects.isNull( unit ) )
        {
            unit = properties.getUnit();
            STRING_CACHE.put( unit, unit );
        }

        String statistic = STRING_CACHE.getIfPresent( properties.getStatistic() );

        if ( Objects.isNull( statistic ) )
        {
            statistic = properties.getStatistic();
            STRING_CACHE.put( statistic, statistic );
        }

        String locationId = STRING_CACHE.getIfPresent( properties.getLocationId() );

        if ( Objects.isNull( locationId ) )
        {
            locationId = properties.getLocationId();
            STRING_CACHE.put( locationId, locationId );
        }

        String timeSeriesId = STRING_CACHE.getIfPresent( properties.getTimeSeriesId() );

        if ( Objects.isNull( timeSeriesId ) )
        {
            timeSeriesId = properties.getTimeSeriesId();
            STRING_CACHE.put( timeSeriesId, timeSeriesId );
        }

        return Properties.builder()
                         .locationId( locationId )
                         .parameterCode( parameterCode )
                         .statistic( statistic )
                         .unit( unit )
                         .timeSeriesId( timeSeriesId )
                         .time( properties.getTime() )
                         .value( properties.getValue() )
                         .build();
    }

}

