package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.SpatialMask;

/**
 * Custom deserializer for a spatial mask.
 *
 * @author James Brown
 */
public class SpatialMaskDeserializer extends JsonDeserializer<SpatialMask>
{
    /** Logger. */
    private static Logger LOGGER = LoggerFactory.getLogger( SpatialMaskDeserializer.class );

    @Override
    public SpatialMask deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        // Mask with extra attributes (the wkt string is always present)
        if ( node.has( "wkt" ) )
        {
            String wktString = node.get( "wkt" )
                                   .asText();

            LOGGER.debug( "Deserialized a spatial mask WKT string: {}.", wktString );

            String name = null;
            Long srid = null;

            if ( node.has( "srid" ) )
            {
                srid = node.get( "srid" )
                           .asLong();
                LOGGER.debug( "Deserialized a spatial mask SRID: {}.", srid );
            }

            if ( node.has( "name" ) )
            {
                name = node.get( "name" )
                           .asText();
                LOGGER.debug( "Deserialized a spatial mask name: {}.", name );
            }

            return new SpatialMask( name, wktString, srid );
        }

        // Mask with WKT string only
        String wktString = node.asText();
        LOGGER.debug( "Deserialized a spatial mask with a WKT string only: {}.", wktString );

        return new SpatialMask( null, wktString, null );
    }


}

