package wres.io.reading.wrds.geography;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.io.NoDataException;

public class WRDSLocationRootDocumentTest
{

    private static final String GOOD_TEST_CASE_V3 = "{\r\n" + 
            "    \"_metrics\": {\r\n" + 
            "        \"location_count\": 1,\r\n" + 
            "        \"model_tracing_api_call\": 0.008093118667602539,\r\n" + 
            "        \"total_request_time\": 1.7286653518676758\r\n" + 
            "    },\r\n" + 
            "    \"_warnings\": [],\r\n" + 
            "    \"_documentation\": {\r\n" + 
            "        \"swagger URL\": \"http://redacted/docs/location/v3.0/swagger/\"\r\n" +
            "    },\r\n" + 
            "    \"deployment\": {\r\n" + 
            "        \"api_url\": \"https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/\",\r\n" +
            "        \"stack\": \"prod\",\r\n" + 
            "        \"version\": \"v3.1.0\"\r\n" + 
            "    },\r\n" + 
            "    \"data_sources\": {\r\n" + 
            "        \"metadata_sources\": [\r\n" + 
            "            \"NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC\",\r\n" + 
            "            \"USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC\"\r\n" + 
            "        ],\r\n" + 
            "        \"crosswalk_datasets\": {\r\n" + 
            "            \"location_nwm_crosswalk_dataset\": {\r\n" + 
            "                \"location_nwm_crosswalk_dataset_id\": \"1.1\",\r\n" + 
            "                \"name\": \"Location NWM Crosswalk v1.1\",\r\n" + 
            "                \"description\": \"Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID\"\r\n" + 
            "            },\r\n" + 
            "            \"nws_usgs_crosswalk_dataset\": {\r\n" + 
            "                \"nws_usgs_crosswalk_dataset_id\": \"1.0\",\r\n" + 
            "                \"name\": \"NWS Station to USGS Gages 1.0\",\r\n" + 
            "                \"description\": \"Authoritative 1.0 dataset mapping NWS Stations to USGS Gages\"\r\n" + 
            "            }\r\n" + 
            "        }\r\n" + 
            "    },\r\n" + 
            "    \"locations\": [\r\n" + 
            "        {\r\n" + 
            "            \"identifiers\": {\r\n" + 
            "                \"nws_lid\": \"OGCN2\",\r\n" + 
            "                \"usgs_site_code\": \"13174500\",\r\n" + 
            "                \"nwm_feature_id\": \"23320100\",\r\n" + 
            "                \"goes_id\": \"F0068458\",\r\n" + 
            "                \"env_can_gage_id\": null\r\n" + 
            "            },\r\n" + 
            "            \"nws_data\": {\r\n" + 
            "                \"name\": \"Wildhorse\",\r\n" + 
            "                \"wfo\": \"LKN\",\r\n" + 
            "                \"rfc\": \"NWRFC\",\r\n" + 
            "                \"geo_rfc\": \"NWRFC\",\r\n" + 
            "                \"latitude\": 41.6888888888889,\r\n" + 
            "                \"longitude\": -115.843888888889,\r\n" + 
            "                \"map_link\": \"https://maps.google.com/maps?t=k&q=loc:41.6888888888889+-115.843888888889\",\r\n" + 
            "                \"horizontal_datum_name\": \"UNK\",\r\n" + 
            "                \"state\": \"Nevada\",\r\n" + 
            "                \"county\": \"Elko\",\r\n" + 
            "                \"county_code\": 32007,\r\n" + 
            "                \"huc\": \"17050104\",\r\n" + 
            "                \"hsa\": \"LKN\",\r\n" + 
            "                \"zero_datum\": 6118.75,\r\n" + 
            "                \"vertical_datum_name\": \"NGVD29\",\r\n" + 
            "                \"rfc_forecast_point\": true,\r\n" + 
            "                \"rfc_defined_fcst_point\": true,\r\n" + 
            "                \"riverpoint\": true\r\n" + 
            "            },\r\n" + 
            "            \"usgs_data\": {\r\n" + 
            "                \"name\": \"OWYHEE RV NR GOLD CK, NV\",\r\n" + 
            "                \"geo_rfc\": \"NWRFC\",\r\n" + 
            "                \"latitude\": 41.68879428,\r\n" + 
            "                \"longitude\": -115.8448067,\r\n" + 
            "                \"map_link\": \"https://maps.google.com/maps?t=k&q=loc:41.68879428+-115.8448067\",\r\n" + 
            "                \"coord_accuracy_code\": \"S\",\r\n" + 
            "                \"latlon_datum_name\": \"NAD83\",\r\n" + 
            "                \"coord_method_code\": \"M\",\r\n" + 
            "                \"state\": \"Nevada\",\r\n" + 
            "                \"huc\": \"17050104\",\r\n" + 
            "                \"site_type\": \"ST\",\r\n" + 
            "                \"altitude\": 6118.75,\r\n" + 
            "                \"alt_accuracy_code\": 1.0,\r\n" + 
            "                \"alt_datum_code\": \"NGVD29\",\r\n" + 
            "                \"alt_method_code\": \"L\",\r\n" + 
            "                \"drainage_area\": 209.0,\r\n" + 
            "                \"drainage_area_units\": \"square miles\",\r\n" + 
            "                \"contrib_drainage_area\": null,\r\n" + 
            "                \"active\": true,\r\n" + 
            "                \"gages_ii_reference\": false\r\n" + 
            "            },\r\n" + 
            "            \"env_can_gage_data\": {\r\n" + 
            "                \"name\": null,\r\n" + 
            "                \"latitude\": null,\r\n" + 
            "                \"longitude\": null,\r\n" + 
            "                \"map_link\": null,\r\n" + 
            "                \"drainage_area\": null,\r\n" + 
            "                \"contrib_drainage_area\": null,\r\n" + 
            "                \"water_course\": null\r\n" + 
            "            },\r\n" + 
            "            \"nws_preferred\": {\r\n" + 
            "                \"name\": \"Wildhorse\",\r\n" + 
            "                \"latitude\": 41.6888888888889,\r\n" + 
            "                \"longitude\": -115.843888888889,\r\n" + 
            "                \"latlon_datum_name\": \"UNK\",\r\n" + 
            "                \"state\": \"Nevada\",\r\n" + 
            "                \"huc\": \"17050104\"\r\n" + 
            "            },\r\n" + 
            "            \"usgs_preferred\": {\r\n" + 
            "                \"name\": \"OWYHEE RV NR GOLD CK, NV\",\r\n" + 
            "                \"latitude\": 41.68879428,\r\n" + 
            "                \"longitude\": -115.8448067,\r\n" + 
            "                \"latlon_datum_name\": \"NAD83\",\r\n" + 
            "                \"state\": \"Nevada\",\r\n" + 
            "                \"huc\": \"17050104\"\r\n" + 
            "            },\r\n" + 
            "            \"upstream_nwm_features\": [\r\n" + 
            "                \"23320108\"\r\n" + 
            "            ],\r\n" + 
            "            \"downstream_nwm_features\": [\r\n" + 
            "                \"23320090\"\r\n" + 
            "            ]\r\n" + 
            "        }\r\n" + 
            "    ]\r\n" + 
            "}";
    

    private static final String CROSSWALK_ONLY_TEST_CASE_V3 = "{\r\n" + 
            "    \"_metrics\": {\r\n" + 
            "        \"location_count\": 1,\r\n" + 
            "        \"model_tracing_api_call\": 0.008665323257446289,\r\n" + 
            "        \"total_request_time\": 0.14869165420532227\r\n" + 
            "    },\r\n" + 
            "    \"_warnings\": [],\r\n" + 
            "    \"_documentation\": {\r\n" + 
            "        \"swagger URL\": \"http://redacted/docs/location/v3.0/swagger/\"\r\n" +
            "    },\r\n" + 
            "    \"deployment\": {\r\n" + 
            "        \"api_url\": \"https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/?identifiers=true\",\r\n" +
            "        \"stack\": \"prod\",\r\n" + 
            "        \"version\": \"v3.1.0\"\r\n" + 
            "    },\r\n" + 
            "    \"data_sources\": {\r\n" + 
            "        \"metadata_sources\": [\r\n" + 
            "            \"NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC\",\r\n" + 
            "            \"USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC\"\r\n" + 
            "        ],\r\n" + 
            "        \"crosswalk_datasets\": {\r\n" + 
            "            \"location_nwm_crosswalk_dataset\": {\r\n" + 
            "                \"location_nwm_crosswalk_dataset_id\": \"1.1\",\r\n" + 
            "                \"name\": \"Location NWM Crosswalk v1.1\",\r\n" + 
            "                \"description\": \"Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID\"\r\n" + 
            "            },\r\n" + 
            "            \"nws_usgs_crosswalk_dataset\": {\r\n" + 
            "                \"nws_usgs_crosswalk_dataset_id\": \"1.0\",\r\n" + 
            "                \"name\": \"NWS Station to USGS Gages 1.0\",\r\n" + 
            "                \"description\": \"Authoritative 1.0 dataset mapping NWS Stations to USGS Gages\"\r\n" + 
            "            }\r\n" + 
            "        }\r\n" + 
            "    },\r\n" + 
            "    \"locations\": [\r\n" + 
            "        {\r\n" + 
            "            \"identifiers\": {\r\n" + 
            "                \"nws_lid\": \"OGCN2\",\r\n" + 
            "                \"usgs_site_code\": \"13174500\",\r\n" + 
            "                \"nwm_feature_id\": \"23320100\",\r\n" + 
            "                \"goes_id\": \"F0068458\",\r\n" + 
            "                \"env_can_gage_id\": null\r\n" + 
            "            },\r\n" + 
            "            \"upstream_nwm_features\": [\r\n" + 
            "                \"23320108\"\r\n" + 
            "            ],\r\n" + 
            "            \"downstream_nwm_features\": [\r\n" + 
            "                \"23320090\"\r\n" + 
            "            ]\r\n" + 
            "        }\r\n" + 
            "    ]\r\n" + 
            "}";

    private static final String NO_LOCATION_DATA_TEST_CASE_V3 = "{\r\n" +
                                                    "    \"_metrics\": {\r\n" +
                                                    "        \"location_count\": 1,\r\n" +
                                                    "        \"model_tracing_api_call\": 0.008093118667602539,\r\n" +
                                                    "        \"total_request_time\": 1.7286653518676758\r\n" +
                                                    "    },\r\n" +
                                                    "    \"_warnings\": [],\r\n" +
                                                    "    \"_documentation\": {\r\n" +
                                                    "        \"swagger URL\": \"http://redacted/docs/location/v3.0/swagger/\"\r\n" +
                                                    "    },\r\n" +
                                                    "    \"deployment\": {\r\n" +
                                                    "        \"api_url\": \"https://redacted/api/location/v3.0/metadata/nws_lid/OGCN2/\",\r\n" +
                                                    "        \"stack\": \"prod\",\r\n" +
                                                    "        \"version\": \"v3.1.0\"\r\n" +
                                                    "    },\r\n" +
                                                    "    \"data_sources\": {\r\n" +
                                                    "        \"metadata_sources\": [\r\n" +
                                                    "            \"NWS data: NRLDB - Last updated: 2021-05-20 19:04:57 UTC\",\r\n" +
                                                    "            \"USGS data: USGS NWIS - Last updated: 2021-05-20 18:04:20 UTC\"\r\n" +
                                                    "        ],\r\n" +
                                                    "        \"crosswalk_datasets\": {\r\n" +
                                                    "            \"location_nwm_crosswalk_dataset\": {\r\n" +
                                                    "                \"location_nwm_crosswalk_dataset_id\": \"1.1\",\r\n" +
                                                    "                \"name\": \"Location NWM Crosswalk v1.1\",\r\n" +
                                                    "                \"description\": \"Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID\"\r\n" +
                                                    "            },\r\n" +
                                                    "            \"nws_usgs_crosswalk_dataset\": {\r\n" +
                                                    "                \"nws_usgs_crosswalk_dataset_id\": \"1.0\",\r\n" +
                                                    "                \"name\": \"NWS Station to USGS Gages 1.0\",\r\n" +
                                                    "                \"description\": \"Authoritative 1.0 dataset mapping NWS Stations to USGS Gages\"\r\n" +
                                                    "            }\r\n" +
                                                    "        }\r\n" +
                                                    "    },\r\n" +
                                                    "    \"locations\": [\r\n" +
                                                    "    ]\r\n" +
                                                    "}";
    
    @Test
    public void readGoodTestCase() throws JsonParseException, JsonMappingException, IOException
    {
        WrdsLocationRootDocument
                dataPoint = new ObjectMapper().readValue( GOOD_TEST_CASE_V3.getBytes(), WrdsLocationRootDocument.class);
        Assert.assertEquals("OGCN2", dataPoint.getLocations().get( 0 ).nwsLid());
        Assert.assertEquals("23320100", dataPoint.getLocations().get( 0 ).nwmFeatureId());
        Assert.assertEquals("13174500", dataPoint.getLocations().get( 0 ).usgsSiteCode());
    }

    @Test
    public void readCrosswalkOnlyTestCase() throws JsonParseException, JsonMappingException, IOException
    {
        WrdsLocationRootDocument dataPoint = new ObjectMapper().readValue( CROSSWALK_ONLY_TEST_CASE_V3.getBytes(), WrdsLocationRootDocument.class);
        Assert.assertEquals("OGCN2", dataPoint.getLocations().get( 0 ).nwsLid());
        Assert.assertEquals("23320100", dataPoint.getLocations().get( 0 ).nwmFeatureId());
        Assert.assertEquals("13174500", dataPoint.getLocations().get( 0 ).usgsSiteCode());
    }

    @Test
    public void readBadTestCaseNoLocationData() throws IOException
    {
        WrdsLocationRootDocument dataPoint = new ObjectMapper().readValue( NO_LOCATION_DATA_TEST_CASE_V3.getBytes(), WrdsLocationRootDocument.class);
        Exception exception = assertThrows( NoDataException.class, dataPoint::getLocations );

        String expectedErrorMessage = "Unable to get wrds location data. Check that the URL is formed correctly";
        String actualErrorMessage = exception.getMessage();

        assertTrue( actualErrorMessage.contains( expectedErrorMessage ) );
    }
}
