package wres.config.deserializers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.JsonNode;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationUtilities;
import wres.config.components.SpatialMask;
import wres.config.components.SpatialMaskBuilder;

/**
 * Custom deserializer for a spatial mask.
 *
 * @author James Brown
 */
public class SpatialMaskDeserializer extends ValueDeserializer<SpatialMask>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SpatialMaskDeserializer.class );

    @Override
    public SpatialMask deserialize( JsonParser jp, DeserializationContext context )
    {
        Objects.requireNonNull( jp );

        ObjectReadContext mapper = jp.objectReadContext();
        JsonNode node = mapper.readTree( jp );

        String wktString;
        String name = null;
        Long srid = null;

        // Mask with extra attributes (the wkt string is always present)
        if ( node.has( "wkt" ) )
        {
            wktString = node.get( "wkt" )
                            .asString();

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
                           .asString();
                LOGGER.debug( "Deserialized a spatial mask name: {}.", name );
            }
        }
        else
        {
            wktString = node.asString();
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
            throw new UncheckedIOException( new IOException( "The 'wkt' string associated with the 'spatial_mask' "
                                                             + "could not be parsed into a geometry. Please fix "
                                                             + "the 'wkt' string and try again. The wkt string is: "
                                                             + wktString, e ) );
        }
    }
}

