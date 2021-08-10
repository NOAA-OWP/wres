package wres.io.geography.wrds.v2;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import wres.datamodel.MissingValues;
import wres.io.geography.wrds.v2.WrdsLocationRootDocument;
import wres.io.reading.wrds.nwm.NwmDataPoint;

public class WRDSLocationRootDocumentTest
{

    private static final String GOOD_TEST_CASE_V2 = "{\r\n" + 
            "    \"_metrics\": {\r\n" + 
            "        \"location_count\": 1,\r\n" + 
            "        \"model_tracing_api_call\": 0.009302377700805664,\r\n" + 
            "        \"total_request_time\": 17.214682817459106\r\n" + 
            "    },\r\n" + 
            "    \"_warnings\": [],\r\n" + 
            "    \"_documentation\": \"redacted/docs/prod/v2/location/swagger/\",\r\n" +
            "    \"locations\": [\r\n" + 
            "        {\r\n" + 
            "            \"nwm_feature_id\": \"23320100\",\r\n" + 
            "            \"goes_id\": \"F0068458\",\r\n" + 
            "            \"usgs_site_code\": \"13174500\",\r\n" + 
            "            \"env_can_gage_id\": \"\",\r\n" + 
            "            \"nws_lid\": \"OGCN2\",\r\n" + 
            "            \"huc\": \"17050104\",\r\n" + 
            "            \"nws_huc\": \"17050104\",\r\n" + 
            "            \"wfo\": \"LKN\",\r\n" + 
            "            \"rfc\": \"CNRFC\",\r\n" + 
            "            \"state\": \"Nevada\",\r\n" + 
            "            \"county\": \"Elko\",\r\n" + 
            "            \"county_code\": \"32007\",\r\n" + 
            "            \"name\": \"OWYHEE RV NR GOLD CK, NV\",\r\n" + 
            "            \"hsa\": \"LKN\",\r\n" + 
            "            \"longitude\": \"-115.8448067\",\r\n" + 
            "            \"latitude\": \"41.68879428\",\r\n" + 
            "            \"site_type\": \"ST\",\r\n" + 
            "            \"rfc_forecast_point\": \"True\",\r\n" + 
            "            \"rfc_defined_fcst_point\": \"True\",\r\n" + 
            "            \"flood_only_forecast_point\": \"\",\r\n" + 
            "            \"gages_ii_reference\": \"False\",\r\n" + 
            "            \"active\": \"True\",\r\n" + 
            "            \"alt_nws_zero_datum\": \"6118.75\",\r\n" + 
            "            \"alt_usgs_gage_altitude\": \"6118.75\",\r\n" + 
            "            \"altitude\": \"6118.75\",\r\n" + 
            "            \"alt_datum_code\": \"NGVD29\",\r\n" + 
            "            \"drainage_area\": \"209.0\",\r\n" + 
            "            \"riverpoint\": \"True\",\r\n" + 
            "            \"contrib_drainage_area\": \"\",\r\n" + 
            "            \"crosswalk_datasets\": {\r\n" + 
            "                \"location_nwm_crosswalk_dataset\": {\r\n" + 
            "                    \"location_nwm_crosswalk_dataset_id\": \"1.1\",\r\n" + 
            "                    \"name\": \"Location NWM Crosswalk v1.1\",\r\n" + 
            "                    \"description\": \"Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID\"\r\n" + 
            "                },\r\n" + 
            "                \"nws_usgs_crosswalk_dataset\": {\r\n" + 
            "                    \"nws_usgs_crosswalk_dataset_id\": \"1.0\",\r\n" + 
            "                    \"name\": \"NWS Station to USGS Gages 1.0\",\r\n" + 
            "                    \"description\": \"Authoritative 1.0 dataset mapping NWS Stations to USGS Gages\"\r\n" + 
            "                }\r\n" + 
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
    
    @Test
    public void readGoodTestCase() throws JsonParseException, JsonMappingException, IOException
    {
        WrdsLocationRootDocument dataPoint = new ObjectMapper().readValue(GOOD_TEST_CASE_V2.getBytes(), WrdsLocationRootDocument.class);
        Assert.assertEquals("OGCN2", dataPoint.getLocations().get( 0 ).getNwsLid());
        Assert.assertEquals("23320100", dataPoint.getLocations().get( 0 ).getNwmFeatureId());
        Assert.assertEquals("13174500", dataPoint.getLocations().get( 0 ).getUsgsSiteCode());
    }
}
