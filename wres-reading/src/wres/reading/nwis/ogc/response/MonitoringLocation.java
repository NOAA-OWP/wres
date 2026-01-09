package wres.reading.nwis.ogc.response;

import java.io.Serial;
import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

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
}
