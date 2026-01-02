package wres.reading.wrds.ahps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonAlias;
import lombok.Getter;
import lombok.Setter;

/**
 * A header.
 */
@JsonIgnoreProperties( ignoreUnknown = true )
@Getter
@Setter
public class Header
{
    private ForecastRequest request;
    @JsonAlias( { "missing_values", "missingValues" } ) 
    private double[] missingValues;
}
