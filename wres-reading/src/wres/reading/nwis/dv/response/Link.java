package wres.reading.nwis.dv.response;

import java.io.Serial;
import java.io.Serializable;
import java.net.URI;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A hyperlink and associated metadata.
 *
 * @author James Brown
 */
@Setter
@Getter
@JsonIgnoreProperties( ignoreUnknown = true )
public class Link implements Serializable
{
    @Serial
    private static final long serialVersionUID = -4618690300625325165L;

    /** The context of the link. */
    private String rel;

    /** The URI. */
    private URI href;

    /**
     * @return a string representation
     */

    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "rel", this.getRel() )
                .append( "href", this.getHref() )
                .toString();
    }
}
