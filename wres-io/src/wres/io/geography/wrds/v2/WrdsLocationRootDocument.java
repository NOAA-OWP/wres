package wres.io.geography.wrds.v2;

import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import wres.io.geography.wrds.WrdsLocation;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Parse relevant portions of a document similar to
 * {
 *     "_metrics": {
 *         "location_count": 1,
 *         "model_tracing_api_call": 0.019634008407592773,
 *         "total_request_time": 0.11055397987365723
 *     },
 *     "_warnings": [],
 *     "_documentation": "redacted/docs/stage/location/swagger/",
 *     "locations": [
 *         {
 *             "nwm_feature_id": "18384141",
 *             "goes_id": "DDD7D016",
 *             "usgs_site_code": "09165000",
 *             "nws_lid": "DRRC2",
 *             "huc": "14030002",
 *             "rfc": "CBRFC",
 *             "state": "Colorado",
 *             "county": "Dolores",
 *             "county_code": "8033",
 *             "name": "DOLORES RIVER BELOW RICO, CO.",
 *             "longitude": "-108.0603517",
 *             "latitude": "37.63888428",
 *             "site_type": "ST",
 *             "rfc_forecast_point": "False",
 *             "flood_only_forecast_point": "False",
 *             "gages_ii_reference": "False",
 *             "active": "True",
 *             "crosswalk_datasets": {
 *                 "nwm_location_crosswalk_dataset": {
 *                     "nwm_location_crosswalk_dataset_id": "2.1-corrected",
 *                     "name": "National Water Model v2.1 Corrected",
 *                     "description": "National Water Model v2.1 Corrected"
 *                 },
 *                 "nws_usgs_crosswalk_dataset": {
 *                     "nws_usgs_crosswalk_dataset_id": "1.0",
 *                     "name": "NWS Station to USGS Gages 1.0",
 *                     "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"
 *                 }
 *             },
 *             "upstream_nwm_features": [
 *                 "18384171",
 *                 "18384129"
 *             ],
 *             "downstream_nwm_features": [
 *                 "18384153"
 *             ]
 *         }
 *     ]
 * }
 */
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class WrdsLocationRootDocument
{
    private final List<WrdsLocation> locations;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public WrdsLocationRootDocument( @JsonProperty( "locations" )
                                     List<WrdsLocation> locations )
    {
        this.locations = locations;
    }

    public List<WrdsLocation> getLocations()
    {
        return this.locations;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "locations", locations )
                .toString();
    }
}
