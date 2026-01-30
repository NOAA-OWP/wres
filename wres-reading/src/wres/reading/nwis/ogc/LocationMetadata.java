package wres.reading.nwis.ogc;

import java.time.ZoneOffset;
import java.util.TimeZone;

import lombok.Builder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;

/**
 * Small value class of location metadata.
 *
 * @author James Brown
 */

@Builder( toBuilder = true )
public record LocationMetadata( Geometry geometry,
                                TimeZone timeZone,
                                ZoneOffset zoneOffset,
                                String description )
{
    @NotNull
    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "Geometry", this.geometry() )
                .append( "Time zone", this.timeZone() )
                .append( "Time zone offset", this.zoneOffset() )
                .append( "Description", this.description() )
                .toString();
    }
}
