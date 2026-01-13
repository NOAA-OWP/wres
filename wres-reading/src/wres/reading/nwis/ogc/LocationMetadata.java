package wres.reading.nwis.ogc;

import java.time.ZoneOffset;
import java.util.TimeZone;

import lombok.Builder;
import org.locationtech.jts.geom.Geometry;

/**
 * Small value class of location metadata.
 *
 * @author James Brown
 */

@Builder( toBuilder = true )
public record LocationMetadata( Geometry geometry, TimeZone timeZone, ZoneOffset zoneOffset )
{
}
