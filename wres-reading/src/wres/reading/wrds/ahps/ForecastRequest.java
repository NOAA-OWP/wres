package wres.reading.wrds.ahps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * A forecast request.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
@Getter
@Setter
public class ForecastRequest
{
    private String url;
    private String path;
}
