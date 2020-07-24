package wres.io.geography.wrds;

import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Parse relevant portions of a a document similar to
 *{
 *    "_metrics": {
 *        "location_count": 1,
 *        "model_tracing_api_call": 0.01062917709350586,
 *        "total_request_time": 0.16103196144104004
 *    },
 *    "_warnings": [],
 *    "_documentation": "***REMOVED***.***REMOVED***.***REMOVED***/docs/prod/location/swagger/",
 *    "locations": [
 *        {
 *            "nwm_features": {
 *                "1.2": {
 *                    "nwm_feature_id": "6163159",
 *                    "dataset": "National Water Model v1.2",
 *                    "dataset_id": "1.2"
 *                },
 *                "2.0": {
 *                    "nwm_feature_id": "6163159",
 *                    "dataset": "National Water Model v2.0",
 *                    "dataset_id": "2.0"
 *                },
 *                "2.1": {
 *                    "nwm_feature_id": "6163159",
 *                    "dataset": "National Water Model v2.1",
 *                    "dataset_id": "2.1"
 *                },
 *                "2.1-corrected": {
 *                    "nwm_feature_id": "6163159",
 *                    "dataset": "National Water Model v2.1 Corrected",
 *                    "dataset_id": "2.1-corrected"
 *                }
 *            },
 *            "goes_id": "CE3984EA",
 *            "usgs_site_code": "01122000",
 *            "nws_lid": "WMNC3",
 *            "huc": "01100002",
 *            "rfc": "NERFC",
 *            "state": "Connecticut",
 *            "county": "Windham",
 *            "county_code": "9015",
 *            "name": "NATCHAUG RIVER AT WILLIMANTIC, CT",
 *            "longitude": "-72.195575",
 *            "latitude": "41.7201",
 *            "upstream_nwm_features": [
 *                "6163349",
 *                "6163155"
 *            ],
 *            "downstream_nwm_features": [
 *                "6162677"
 *            ]
 *        }
 *    ]
 *}
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
