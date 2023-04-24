package wres.io.reading.wrds.ahps;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A forecast response.
 */
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

    /**
     * @return the status code
     */
    public short getStatusCode()
    {
        return statusCode;
    }

    /**
     * @return the message
     */
    public String getMessage()
    {
        return message;
    }

    /**
     * @return the header
     */
    public Header getHeader()
    {
        return header;
    }

    /**
     * Sets the header.
     * @param header the header
     */
    public void setHeader( Header header )
    {
        this.header = header;
    }

    /**
     * Sets the message.
     * @param message the message
     */
    public void setMessage( String message )
    {
        this.message = message;
    }

    /**
     * Sets the status code.
     * @param statusCode tje status code
     */
    public void setStatusCode( short statusCode )
    {
        this.statusCode = statusCode;
    }

    /**
     * @return the forecasts
     */
    public Forecast[] getForecasts()
    {
        return forecasts;
    }

    /**
     * Sets the forecasts.
     * @param forecasts the forecasts
     */
    public void setForecasts( Forecast[] forecasts )
    {
        this.forecasts = forecasts;
    }
}
