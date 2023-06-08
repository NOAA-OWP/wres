package wres.config.yaml.deserializers;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.SpatialMaskBuilder;

/**
 * Custom deserializer for a spatial mask.
 *
 * @author James Brown
 */
public class SpatialMaskDeserializer extends JsonDeserializer<SpatialMask>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SpatialMaskDeserializer.class );

    @Override
    public SpatialMask deserialize( JsonParser jp, DeserializationContext context )
            throws IOException
    {
        Objects.requireNonNull( jp );

        ObjectReader mapper = ( ObjectReader ) jp.getCodec();
        JsonNode node = mapper.readTree( jp );

        String wktString;
        String name = null;
        Long srid = null;

        // Mask with extra attributes (the wkt string is always present)
        if ( node.has( "wkt" ) )
        {
            wktString = node.get( "wkt" )
                            .asText();

            LOGGER.debug( "Deserialized a spatial mask WKT string: {}.", wktString );

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
        }
        else
        {
            wktString = node.asText();
            LOGGER.debug( "Deserialized a spatial mask with a WKT string only: {}.", wktString );
        }

        try
        {
            Geometry geometry = DeclarationUtilities.getGeometry( wktString, srid );

            SpatialMaskBuilder builder = SpatialMaskBuilder.builder()
                                                           .geometry( geometry );

            if ( Objects.nonNull( name ) )
            {
                builder.name( name );
            }

            return builder.build();
        }
        catch ( IllegalArgumentException e )
        {
            throw new IOException( "The 'wkt' string associated with the 'spatial_mask' "
                                   + "could not be parsed into a geometry. Please fix "
                                   + "the 'wkt' string and try again. The wkt string is: "
                                   + wktString, e );
        }
    }
}

