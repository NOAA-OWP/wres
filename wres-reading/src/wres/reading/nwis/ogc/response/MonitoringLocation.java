package wres.reading.nwis.ogc.response;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import tools.jackson.databind.annotation.JsonDeserialize;

/**
 * Represents metadata for a specific feature or "monitoring location" from a monitoring-locations endpoint.
 *
 * @author James Brown
 */
@Setter
@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
public class MonitoringLocation implements Serializable
{
    @Serial
    private static final long serialVersionUID = -134563188851878254L;

    /** The monitoring location properties. */
    private MonitoringLocationProperties properties;

    /** The monitoring location identifier. */
    private String id;

    /** The geometry. */
    @JsonDeserialize( using = GeometryDeserializer.class )
    private org.locationtech.jts.geom.Geometry geometry;

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "id", this.getId() )
                .append( "geometry", this.getGeometry() )
                .append( "properties", this.getProperties() )
                .toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !( o instanceof MonitoringLocation location ) )
        {
            return false;
        }

        return Objects.equals( location.id, this.getId() )
               && Objects.equals( location.geometry, this.getGeometry() )
               && Objects.equals( location.properties, this.getProperties() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getId(), this.getGeometry(), this.getProperties() );
    }
}
