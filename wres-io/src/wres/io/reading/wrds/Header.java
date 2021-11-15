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

    public double[] getMissing_values()
    {
        return missing_values;
    }

    public void setMissing_values( double[] missing_values)
    {
        this.missing_values = missing_values;
    }

    ForecastRequest request;

    double[] missing_values;
}
