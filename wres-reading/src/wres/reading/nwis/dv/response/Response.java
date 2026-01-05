package wres.reading.nwis.dv.response;

import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;

import jakarta.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A response.
 *
 * @author James Brown
 */
@Setter
@Getter
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class Response implements Serializable
{
    @Serial
    private static final long serialVersionUID = 8873948278024424507L;

    /** The features, each one containing a single time-series value. */
    private Feature[] features;

    /** The number of features returned. */
    private long numberReturned;

    /** Links. */
    Link[] links;

    /**
     * @return a string representation
     */

    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "features", Arrays.toString( this.getFeatures() ) )
                .append( "links", Arrays.toString( this.getLinks() ) )
                .append( "numberReturned", this.getNumberReturned() )
                .toString();
    }
}
