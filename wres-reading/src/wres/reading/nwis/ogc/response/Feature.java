package wres.reading.nwis.ogc.response;

import java.io.Serial;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tools.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A geographic feature that contains a single time-series value.
 *
 * @author James Brown
 */

@Setter
@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
public class Feature implements Serializable
{
    @Serial
    private static final long serialVersionUID = 3806690683062276506L;

    /** The properties of the feature. */
    private Properties properties;

    /** The geometry, which is read optionally. This is unused when reading location metadata centrally, from the
     * monitoring location endpoint, for efficiency. */
    @JsonDeserialize( using = GeometryDeserializer.class )
    private org.locationtech.jts.geom.Geometry geometry;

    /**
     * @return a string representation
     */

    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "properties", this.getProperties() )
                .append( "geometry", this.getGeometry() )
                .toString();
    }
}
