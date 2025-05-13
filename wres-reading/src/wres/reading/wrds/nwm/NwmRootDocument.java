package wres.reading.wrds.nwm;

import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import wres.reading.wrds.ahps.ParameterCodes;

import lombok.Getter;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Parse a document similar to
 * {
 *     "forecasts": [
 *         {
 *             "reference_time": "20200108T18Z",
 *             "features": [
 *                 {
 *                     "location": {
 *                         "names": {
 *                             "nws_lid": "CHAF1",
 *                             "usgs_site_code": "02358000",
 *                             "nwm_feature_id": "2293124",
 *                             "name": "APALACHICOLA RIVER AT CHATTAHOOCHEE FLA"
 *                         },
 *                         "coordinates": {
 *                             "latitude": "30.701",
 *                             "longitude": "-84.8591"
 *                         }
 *                     },
 *                     "members": [
 *                         {
 *                             "identifier": "1",
 *                             "data_points": [
 *                                 {
 *                                     "time": "20200108T21Z",
 *                                     "value": "1432.8399679735303"
 *                                 },
 *                                 {
 *                                     "time": "20200108T20Z",
 *                                     "value": "1430.0099680367857"
 *                                 },
 *                                 {
 *                                     "time": "20200108T19Z",
 *                                     "value": "1427.1799681000412"
 *                                 }
 *                             ]
 *                         }
 *                     ]
 *                 },
 * ...
 * }
 */
@Getter
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmRootDocument
{
    /** The forecasts. */
    private final List<NwmForecast> forecasts;
    /** The variable. */
    private final Map<String, String> variable;
    /** The parameter codes. */
    private final ParameterCodes parameterCodes;
    /** The warnings. */
    private final List<String> warnings;

    /**
     * Creates an instance.
     * @param forecasts the forecasts
     * @param variable the variable
     * @param parameterCodes the parameter codes
     * @param warnings the warnings
     */
    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmRootDocument( @JsonProperty( "forecasts" )
                            List<NwmForecast> forecasts,
                            @JsonProperty( "variable" )
                            Map<String, String> variable,
                            @JsonProperty( "parameter_codes" )
                            ParameterCodes parameterCodes,
                            @JsonProperty( "_warnings" )
                            List<String> warnings )
    {
        this.forecasts = forecasts;
        this.variable = variable;
        this.parameterCodes = parameterCodes;
        this.warnings = warnings;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "forecasts", forecasts )
                .append( "variable", variable )
                .append( "parameterCodes", parameterCodes )
                .append( "warnings", warnings )
                .toString();
    }
}
