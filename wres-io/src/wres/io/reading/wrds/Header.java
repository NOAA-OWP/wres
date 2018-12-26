package wres.io.reading.wrds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties( ignoreUnknown = true )
public class Header
{
    public ForecastRequest getRequest()
    {
        return request;
    }

    public void setRequest( ForecastRequest request )
    {
        this.request = request;
    }

    ForecastRequest request;
}
