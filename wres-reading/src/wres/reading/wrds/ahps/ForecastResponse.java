package wres.reading.wrds.ahps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Getter;
import lombok.Setter;

/**
 * A forecast response.
 */
@Getter
@Setter
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class ForecastResponse
{
    @JsonAlias( { "timeseriesDataset" } )
    private Forecast[] forecasts;

    @JsonProperty( "status_code" )
    private short statusCode;

    private String message;

    private Header header;
}
