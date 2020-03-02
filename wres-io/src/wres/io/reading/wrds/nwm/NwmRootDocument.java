package wres.io.reading.wrds.nwm;

import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
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
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class NwmRootDocument
{
    private final List<NwmForecast> forecasts;
    private final Map<String,String> variable;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public NwmRootDocument( @JsonProperty( "forecasts" )
                            List<NwmForecast> forecasts,
                            @JsonProperty( "variable" )
                            Map<String,String> variable )
    {
        this.forecasts = forecasts;
        this.variable = variable;
    }

    public List<NwmForecast> getForecasts()
    {
        return this.forecasts;
    }

    public Map<String,String> getVariable()
    {
        return this.variable;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "forecasts", forecasts )
                .toString();
    }
}
