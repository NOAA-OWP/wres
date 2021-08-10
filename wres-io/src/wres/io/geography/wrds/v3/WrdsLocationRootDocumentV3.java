package wres.io.geography.wrds.v3;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import wres.io.geography.wrds.WrdsLocation;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/*
Parse relevant portions of a a document similar to this (which is generated when 
"?identifiers=true" is included at the end of the URL: 
{
    "_metrics": {
        "location_count": 1,
        "model_tracing_api_call": 0.008665323257446289,
        "total_request_time": 0.14869165420532227
    },
    "_warnings": [],
    "_documentation": {
        "swagger URL": "http://redacted/docs/location/v3.0/swagger/"
    },
    "deployment": {
        "api_url": "https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/?identifiers=true",
        "stack": "prod",
        "version": "v3.1.0"
    },
    "data_sources": {
        "metadata_sources": [
            "NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC",
            "USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC"
        ],
        "crosswalk_datasets": {
            "location_nwm_crosswalk_dataset": {
                "location_nwm_crosswalk_dataset_id": "1.1",
                "name": "Location NWM Crosswalk v1.1",
                "description": "Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID"
            },
            "nws_usgs_crosswalk_dataset": {
                "nws_usgs_crosswalk_dataset_id": "1.0",
                "name": "NWS Station to USGS Gages 1.0",
                "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"
            }
        }
    },
    "locations": [
        {
            "identifiers": {
                "nws_lid": "OGCN2",
                "usgs_site_code": "13174500",
                "nwm_feature_id": "23320100",
                "goes_id": "F0068458",
                "env_can_gage_id": null
            },
            "upstream_nwm_features": [
                "23320108"
            ],
            "downstream_nwm_features": [
                "23320090"
            ]
        }
    ]
}

OR this when "?identiers=true" is NOT included at the end, resulting in full output:

{
    "_metrics": {
        "location_count": 1,
        "model_tracing_api_call": 0.008093118667602539,
        "total_request_time": 1.7286653518676758
    },
    "_warnings": [],
    "_documentation": {
        "swagger URL": "http://redacted/docs/location/v3.0/swagger/"
    },
    "deployment": {
        "api_url": "https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/",
        "stack": "prod",
        "version": "v3.1.0"
    },
    "data_sources": {
        "metadata_sources": [
            "NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC",
            "USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC"
        ],
        "crosswalk_datasets": {
            "location_nwm_crosswalk_dataset": {
                "location_nwm_crosswalk_dataset_id": "1.1",
                "name": "Location NWM Crosswalk v1.1",
                "description": "Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID"
            },
            "nws_usgs_crosswalk_dataset": {
                "nws_usgs_crosswalk_dataset_id": "1.0",
                "name": "NWS Station to USGS Gages 1.0",
                "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"
            }
        }
    },
    "locations": [
        {
            "identifiers": {
                "nws_lid": "OGCN2",
                "usgs_site_code": "13174500",
                "nwm_feature_id": "23320100",
                "goes_id": "F0068458",
                "env_can_gage_id": null
            },
            "nws_data": {
                "name": "Wildhorse",
                "wfo": "LKN",
                "rfc": "NWRFC",
                "geo_rfc": "NWRFC",
                "latitude": 41.6888888888889,
                "longitude": -115.843888888889,
                "map_link": "https://maps.google.com/maps?t=k&q=loc:41.6888888888889+-115.843888888889",
                "horizontal_datum_name": "UNK",
                "state": "Nevada",
                "county": "Elko",
                "county_code": 32007,
                "huc": "17050104",
                "hsa": "LKN",
                "zero_datum": 6118.75,
                "vertical_datum_name": "NGVD29",
                "rfc_forecast_point": true,
                "rfc_defined_fcst_point": true,
                "riverpoint": true
            },
            "usgs_data": {
                "name": "OWYHEE RV NR GOLD CK, NV",
                "geo_rfc": "NWRFC",
                "latitude": 41.68879428,
                "longitude": -115.8448067,
                "map_link": "https://maps.google.com/maps?t=k&q=loc:41.68879428+-115.8448067",
                "coord_accuracy_code": "S",
                "latlon_datum_name": "NAD83",
                "coord_method_code": "M",
                "state": "Nevada",
                "huc": "17050104",
                "site_type": "ST",
                "altitude": 6118.75,
                "alt_accuracy_code": 1.0,
                "alt_datum_code": "NGVD29",
                "alt_method_code": "L",
                "drainage_area": 209.0,
                "drainage_area_units": "square miles",
                "contrib_drainage_area": null,
                "active": true,
                "gages_ii_reference": false
            },
            "env_can_gage_data": {
                "name": null,
                "latitude": null,
                "longitude": null,
                "map_link": null,
                "drainage_area": null,
                "contrib_drainage_area": null,
                "water_course": null
            },
            "nws_preferred": {
                "name": "Wildhorse",
                "latitude": 41.6888888888889,
                "longitude": -115.843888888889,
                "latlon_datum_name": "UNK",
                "state": "Nevada",
                "huc": "17050104"
            },
            "usgs_preferred": {
                "name": "OWYHEE RV NR GOLD CK, NV",
                "latitude": 41.68879428,
                "longitude": -115.8448067,
                "latlon_datum_name": "NAD83",
                "state": "Nevada",
                "huc": "17050104"
            },
            "upstream_nwm_features": [
                "23320108"
            ],
            "downstream_nwm_features": [
                "23320090"
            ]
        }
    ]
}
 */
@XmlRootElement
@JsonIgnoreProperties( ignoreUnknown = true )
public class WrdsLocationRootDocumentV3
{
    private final List<WrdsLocationInformation> locationInfos;

    @JsonCreator( mode = JsonCreator.Mode.PROPERTIES )
    public WrdsLocationRootDocumentV3( @JsonProperty( "locations" )
                                     List<WrdsLocationInformation> locationInfos )
    {
        this.locationInfos = locationInfos;
    }

    
    /**
     * Pass through the locations, extracting the identifier information, and 
     * returning it in a list.
     * @return List of {@link WrdsLocation} instances.
     */
    public List<WrdsLocation> getLocations()
    {
        List<WrdsLocation> locations = new ArrayList<>();
        for (WrdsLocationInformation info: this.locationInfos)
        {
            locations.add( info.getLocations() );
        }
        return locations;
    }

    public List<WrdsLocationInformation> getLocationInfos()
    {
        return locationInfos;
    }


    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "locations", locationInfos )
                .toString();
    }
}
