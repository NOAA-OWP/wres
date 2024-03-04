package wres.reading.wrds.ahps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;

/**
 * A header.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
public class Header
{
    private ForecastRequest request;
    @JsonAlias( { "missing_values", "missingValues" } ) 
    private double[] missingValues;

    /**
     * @return the forecast request
     */
    public ForecastRequest getRequest()
    {
        return request;
    }

    /**
     * Sets the forecast request.
     * @param request the request
     */
    public void setRequest( ForecastRequest request )
    {
        this.request = request;
    }

    /**
     * @return the missing values
     */
    public double[] getMissingValues()
    {
        return missingValues;
    }

    /**
     * Sets the missing values.
     * @param missingValues the missing values
     */
    public void setMissingValues( double[] missingValues )
    {
        this.missingValues = missingValues;
    }
}
