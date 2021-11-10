package wres.io.reading.wrds.nwm;

import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Parse a document similar to
 * {
 *     "error": "API Currently only supports querying by the following: ('nwm_feature_id', 'nws_lid', 'usgs_site_code', 'state', 'rfc', 'huc', 'county', 'tag')"
 * }
 */
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmRootDocumentWithError
{
    private final Map<String,String> messages;

    @JsonCreator()
    public NwmRootDocumentWithError( Map<String,String> messages )
    {
        this.messages = messages;
    }

    public Map<String,String> getMessages()
    {
        return this.messages;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "messages", messages )
                .toString();
    }
}
