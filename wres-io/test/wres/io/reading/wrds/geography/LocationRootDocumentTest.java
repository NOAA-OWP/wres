package wres.io.reading.wrds.geography;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.NoDataException;

class LocationRootDocumentTest
{
    private static final String GOOD_TEST_CASE_V3 = """
            {\r
                "_metrics": {\r
                    "location_count": 1,\r
                    "model_tracing_api_call": 0.008093118667602539,\r
                    "total_request_time": 1.7286653518676758\r
                },\r
                "_warnings": [],\r
                "_documentation": {\r
                    "swagger URL": "http://redacted/docs/location/v3.0/swagger/"\r
                },\r
                "deployment": {\r
                    "api_url": "https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/",\r
                    "stack": "prod",\r
                    "version": "v3.1.0"\r
                },\r
                "data_sources": {\r
                    "metadata_sources": [\r
                        "NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC",\r
                        "USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC"\r
                    ],\r
                    "crosswalk_datasets": {\r
                        "location_nwm_crosswalk_dataset": {\r
                            "location_nwm_crosswalk_dataset_id": "1.1",\r
                            "name": "Location NWM Crosswalk v1.1",\r
                            "description": "Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID"\r
                        },\r
                        "nws_usgs_crosswalk_dataset": {\r
                            "nws_usgs_crosswalk_dataset_id": "1.0",\r
                            "name": "NWS Station to USGS Gages 1.0",\r
                            "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"\r
                        }\r
                    }\r
                },\r
                "locations": [\r
                    {\r
                        "identifiers": {\r
                            "nws_lid": "OGCN2",\r
                            "usgs_site_code": "13174500",\r
                            "nwm_feature_id": "23320100",\r
                            "goes_id": "F0068458",\r
                            "env_can_gage_id": null\r
                        },\r
                        "nws_data": {\r
                            "name": "Wildhorse",\r
                            "wfo": "LKN",\r
                            "rfc": "NWRFC",\r
                            "geo_rfc": "NWRFC",\r
                            "latitude": 41.6888888888889,\r
                            "longitude": -115.843888888889,\r
                            "map_link": "https://maps.google.com/maps?t=k&q=loc:41.6888888888889+-115.843888888889",\r
                            "horizontal_datum_name": "UNK",\r
                            "state": "Nevada",\r
                            "county": "Elko",\r
                            "county_code": 32007,\r
                            "huc": "17050104",\r
                            "hsa": "LKN",\r
                            "zero_datum": 6118.75,\r
                            "vertical_datum_name": "NGVD29",\r
                            "rfc_forecast_point": true,\r
                            "rfc_defined_fcst_point": true,\r
                            "riverpoint": true\r
                        },\r
                        "usgs_data": {\r
                            "name": "OWYHEE RV NR GOLD CK, NV",\r
                            "geo_rfc": "NWRFC",\r
                            "latitude": 41.68879428,\r
                            "longitude": -115.8448067,\r
                            "map_link": "https://maps.google.com/maps?t=k&q=loc:41.68879428+-115.8448067",\r
                            "coord_accuracy_code": "S",\r
                            "latlon_datum_name": "NAD83",\r
                            "coord_method_code": "M",\r
                            "state": "Nevada",\r
                            "huc": "17050104",\r
                            "site_type": "ST",\r
                            "altitude": 6118.75,\r
                            "alt_accuracy_code": 1.0,\r
                            "alt_datum_code": "NGVD29",\r
                            "alt_method_code": "L",\r
                            "drainage_area": 209.0,\r
                            "drainage_area_units": "square miles",\r
                            "contrib_drainage_area": null,\r
                            "active": true,\r
                            "gages_ii_reference": false\r
                        },\r
                        "env_can_gage_data": {\r
                            "name": null,\r
                            "latitude": null,\r
                            "longitude": null,\r
                            "map_link": null,\r
                            "drainage_area": null,\r
                            "contrib_drainage_area": null,\r
                            "water_course": null\r
                        },\r
                        "nws_preferred": {\r
                            "name": "Wildhorse",\r
                            "latitude": 41.6888888888889,\r
                            "longitude": -115.843888888889,\r
                            "latlon_datum_name": "UNK",\r
                            "state": "Nevada",\r
                            "huc": "17050104"\r
                        },\r
                        "usgs_preferred": {\r
                            "name": "OWYHEE RV NR GOLD CK, NV",\r
                            "latitude": 41.68879428,\r
                            "longitude": -115.8448067,\r
                            "latlon_datum_name": "NAD83",\r
                            "state": "Nevada",\r
                            "huc": "17050104"\r
                        },\r
                        "upstream_nwm_features": [\r
                            "23320108"\r
                        ],\r
                        "downstream_nwm_features": [\r
                            "23320090"\r
                        ]\r
                    }\r
                ]\r
            }""";
    

    private static final String CROSSWALK_ONLY_TEST_CASE_V3 = """
            {\r
                "_metrics": {\r
                    "location_count": 1,\r
                    "model_tracing_api_call": 0.008665323257446289,\r
                    "total_request_time": 0.14869165420532227\r
                },\r
                "_warnings": [],\r
                "_documentation": {\r
                    "swagger URL": "http://redacted/docs/location/v3.0/swagger/"\r
                },\r
                "deployment": {\r
                    "api_url": "https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/?identifiers=true",\r
                    "stack": "prod",\r
                    "version": "v3.1.0"\r
                },\r
                "data_sources": {\r
                    "metadata_sources": [\r
                        "NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC",\r
                        "USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC"\r
                    ],\r
                    "crosswalk_datasets": {\r
                        "location_nwm_crosswalk_dataset": {\r
                            "location_nwm_crosswalk_dataset_id": "1.1",\r
                            "name": "Location NWM Crosswalk v1.1",\r
                            "description": "Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID"\r
                        },\r
                        "nws_usgs_crosswalk_dataset": {\r
                            "nws_usgs_crosswalk_dataset_id": "1.0",\r
                            "name": "NWS Station to USGS Gages 1.0",\r
                            "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"\r
                        }\r
                    }\r
                },\r
                "locations": [\r
                    {\r
                        "identifiers": {\r
                            "nws_lid": "OGCN2",\r
                            "usgs_site_code": "13174500",\r
                            "nwm_feature_id": "23320100",\r
                            "goes_id": "F0068458",\r
                            "env_can_gage_id": null\r
                        },\r
                        "upstream_nwm_features": [\r
                            "23320108"\r
                        ],\r
                        "downstream_nwm_features": [\r
                            "23320090"\r
                        ]\r
                    }\r
                ]\r
            }""";

    private static final String NULL_LOCATION_DATA_TEST_CASE_V3 = """
            {\r
                "_metrics": {\r
                    "location_count": 1,\r
                    "model_tracing_api_call": 0.008093118667602539,\r
                    "total_request_time": 1.7286653518676758\r
                },\r
                "_warnings": [],\r
                "_documentation": {\r
                    "swagger URL": "http://redacted/docs/location/v3.0/swagger/"\r
                },\r
                "deployment": {\r
                    "api_url": "https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/",\r
                    "stack": "prod",\r
                    "version": "v3.1.0"\r
                },\r
                "data_sources": {\r
                    "metadata_sources": [\r
                        "NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC",\r
                        "USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC"\r
                    ],\r
                    "crosswalk_datasets": {\r
                        "location_nwm_crosswalk_dataset": {\r
                            "location_nwm_crosswalk_dataset_id": "1.1",\r
                            "name": "Location NWM Crosswalk v1.1",\r
                            "description": "Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID"\r
                        },\r
                        "nws_usgs_crosswalk_dataset": {\r
                            "nws_usgs_crosswalk_dataset_id": "1.0",\r
                            "name": "NWS Station to USGS Gages 1.0",\r
                            "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"\r
                        }\r
                    }\r
                }\r
            }""";

    private static final String NO_LOCATION_DATA_TEST_CASE_V3 = """
            {\r
                "_metrics": {\r
                    "location_count": 1,\r
                    "model_tracing_api_call": 0.008093118667602539,\r
                    "total_request_time": 1.7286653518676758\r
                },\r
                "_warnings": [],\r
                "_documentation": {\r
                    "swagger URL": "http://redacted/docs/location/v3.0/swagger/"\r
                },\r
                "deployment": {\r
                    "api_url": "https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/",\r
                    "stack": "prod",\r
                    "version": "v3.1.0"\r
                },\r
                "data_sources": {\r
                    "metadata_sources": [\r
                        "NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC",\r
                        "USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC"\r
                    ],\r
                    "crosswalk_datasets": {\r
                        "location_nwm_crosswalk_dataset": {\r
                            "location_nwm_crosswalk_dataset_id": "1.1",\r
                            "name": "Location NWM Crosswalk v1.1",\r
                            "description": "Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID"\r
                        },\r
                        "nws_usgs_crosswalk_dataset": {\r
                            "nws_usgs_crosswalk_dataset_id": "1.0",\r
                            "name": "NWS Station to USGS Gages 1.0",\r
                            "description": "Authoritative 1.0 dataset mapping NWS Stations to USGS Gages"\r
                        }\r
                    }\r
                },\r
                "locations": [\r
                ]\r
            }""";
    
    @Test
    void readGoodTestCase() throws IOException
    {
        LocationRootDocument
                dataPoint = new ObjectMapper().readValue( GOOD_TEST_CASE_V3.getBytes(), LocationRootDocument.class);
        assertEquals("OGCN2", dataPoint.getLocations().get( 0 ).nwsLid());
        assertEquals("23320100", dataPoint.getLocations().get( 0 ).nwmFeatureId());
        assertEquals("13174500", dataPoint.getLocations().get( 0 ).usgsSiteCode());
    }

    @Test
    void readCrosswalkOnlyTestCase() throws IOException
    {
        LocationRootDocument
                dataPoint = new ObjectMapper().readValue( CROSSWALK_ONLY_TEST_CASE_V3.getBytes(), LocationRootDocument.class);
        assertEquals("OGCN2", dataPoint.getLocations().get( 0 ).nwsLid());
        assertEquals("23320100", dataPoint.getLocations().get( 0 ).nwmFeatureId());
        assertEquals("13174500", dataPoint.getLocations().get( 0 ).usgsSiteCode());
    }

    @Test
    void readBadTestCaseNoLocationData() throws IOException
    {
        LocationRootDocument dataPoint = new ObjectMapper().readValue( NO_LOCATION_DATA_TEST_CASE_V3.getBytes(), LocationRootDocument.class);
        Exception exception = assertThrows( NoDataException.class, dataPoint::getLocations );

        String expectedErrorMessage = "Unable to get WRDS location data. Check that the URL is formed correctly.";
        String actualErrorMessage = exception.getMessage();

        assertTrue( actualErrorMessage.contains( expectedErrorMessage ) );
    }

    @Test
    void readBadTestCaseNullLocationData() throws IOException
    {
        LocationRootDocument dataPoint = new ObjectMapper().readValue( NULL_LOCATION_DATA_TEST_CASE_V3.getBytes(), LocationRootDocument.class);
        Exception exception = assertThrows( NoDataException.class, dataPoint::getLocations );

        String expectedErrorMessage = "Unable to get WRDS location data. Check that the URL is formed correctly.";
        String actualErrorMessage = exception.getMessage();

        assertTrue( actualErrorMessage.contains( expectedErrorMessage ) );
    }
}
