package wres.io.reading.wrds.geography.v2;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.NoDataException;

class WRDSLocationRootDocumentTest
{
    private static final String GOOD_TEST_CASE_V2 = """
            {\r
                "_metrics": {\r
                    "location_count": 1,\r
                    "model_tracing_api_call": 0.009302377700805664,\r
                    "total_request_time": 17.214682817459106\r
                },\r
                "_warnings": [],\r
                "_documentation": "redacted/docs/prod/v2/location/swagger/",\r
                "locations": [\r
                    {\r
                        "nwm_feature_id": "23320100",\r
                        "goes_id": "F0068458",\r
                        "usgs_site_code": "13174500",\r
                        "env_can_gage_id": "",\r
                        "nws_lid": "OGCN2",\r
                        "huc": "17050104",\r
                        "nws_huc": "17050104",\r
                        "wfo": "LKN",\r
                        "rfc": "CNRFC",\r
                        "state": "Nevada",\r
                        "county": "Elko",\r
                        "county_code": "32007",\r
                        "name": "OWYHEE RV NR GOLD CK, NV",\r
                        "hsa": "LKN",\r
                        "longitude": "-115.8448067",\r
                        "latitude": "41.68879428",\r
                        "site_type": "ST",\r
                        "rfc_forecast_point": "True",\r
                        "rfc_defined_fcst_point": "True",\r
                        "flood_only_forecast_point": "",\r
                        "gages_ii_reference": "False",\r
                        "active": "True",\r
                        "alt_nws_zero_datum": "6118.75",\r
                        "alt_usgs_gage_altitude": "6118.75",\r
                        "altitude": "6118.75",\r
                        "alt_datum_code": "NGVD29",\r
                        "drainage_area": "209.0",\r
                        "riverpoint": "True",\r
                        "contrib_drainage_area": "",\r
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

    private static final String NO_LOCATION_DATA_TEST_CASE = """
            {\r
                "_metrics": {\r
                    "location_count": 1,\r
                    "model_tracing_api_call": 0.009302377700805664,\r
                    "total_request_time": 17.214682817459106\r
                },\r
                "_warnings": [],\r
                "_documentation": "redacted/docs/prod/v2/location/swagger/",\r
                "locations": [\r
                ]\r
            }""";
    
    @Test
    void readGoodTestCase() throws IOException
    {
        WrdsLocationRootDocument dataPoint = new ObjectMapper().readValue(GOOD_TEST_CASE_V2.getBytes(), WrdsLocationRootDocument.class);
        assertEquals("OGCN2", dataPoint.getLocations().get( 0 ).nwsLid());
        assertEquals("23320100", dataPoint.getLocations().get( 0 ).nwmFeatureId());
        assertEquals("13174500", dataPoint.getLocations().get( 0 ).usgsSiteCode());
    }

    @Test
    void readBadTestCaseNoLocationData() throws IOException
    {
        WrdsLocationRootDocument dataPoint = new ObjectMapper().readValue(NO_LOCATION_DATA_TEST_CASE.getBytes(), WrdsLocationRootDocument.class);
        Exception exception = assertThrows( NoDataException.class, dataPoint::getLocations );

        String expectedErrorMessage = "Unable to get wrds location data. Check that the URL is formed correctly";
        String actualErrorMessage = exception.getMessage();

        assertTrue( actualErrorMessage.contains( expectedErrorMessage ) );
    }
}
