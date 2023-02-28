package wres.io.thresholds.wrds;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.config.generated.*;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.io.geography.wrds.WrdsLocation;
import wres.datamodel.units.UnitMapper;
import wres.io.thresholds.wrds.v3.GeneralThresholdDefinition;
import wres.io.thresholds.wrds.v3.GeneralThresholdResponse;
import wres.system.SystemSettings;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class GeneralWRDSReaderTest
{
    private static final String TEST_JSON = "test.json";
    private static final String TEST = "test";

    // This response originates from v1 of the API.  URL unknown.
    private static final String RESPONSE = "{\r\n"
                                           + "    \"_documentation\": \"redacted/docs/stage/location/swagger/\",\r\n"
                                           + "    \"_metrics\": {\r\n"
                                           + "        \"threshold_count\": 13,\r\n"
                                           + "        \"total_request_time\": 0.256730318069458\r\n"
                                           + "    },\r\n"
                                           + "    \"thresholds\": [\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"PTSA1\",\r\n"
                                           + "                \"nws_lid\": \"PTSA1\",\r\n"
                                           + "                \"usgs_site_code\": \"02372250\",\r\n"
                                           + "                \"nwm_feature_id\": \"2323396\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"0.0\",\r\n"
                                           + "                \"minor_stage\": \"0.0\",\r\n"
                                           + "                \"moderate_stage\": \"0.0\",\r\n"
                                           + "                \"major_stage\": \"0.0\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"MNTG1\",\r\n"
                                           + "                \"nws_lid\": \"MNTG1\",\r\n"
                                           + "                \"usgs_site_code\": \"02349605\",\r\n"
                                           + "                \"nwm_feature_id\": \"6444276\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"11.0\",\r\n"
                                           + "                \"minor_stage\": \"20.0\",\r\n"
                                           + "                \"moderate_stage\": \"28.0\",\r\n"
                                           + "                \"major_stage\": \"31.0\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"MNTG1\",\r\n"
                                           + "                \"nws_lid\": \"MNTG1\",\r\n"
                                           + "                \"usgs_site_code\": \"02349605\",\r\n"
                                           + "                \"nwm_feature_id\": \"6444276\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"11.0\",\r\n"
                                           + "                \"action_stage\": \"11.0\",\r\n"
                                           + "                \"minor_stage\": \"20.0\",\r\n"
                                           + "                \"moderate_stage\": \"28.0\",\r\n"
                                           + "                \"major_stage\": \"31.0\",\r\n"
                                           + "                \"record_stage\": \"34.11\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"BLOF1\",\r\n"
                                           + "                \"nws_lid\": \"BLOF1\",\r\n"
                                           + "                \"usgs_site_code\": \"02358700\",\r\n"
                                           + "                \"nwm_feature_id\": \"2297254\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"13.0\",\r\n"
                                           + "                \"minor_stage\": \"17.0\",\r\n"
                                           + "                \"moderate_stage\": \"23.5\",\r\n"
                                           + "                \"major_stage\": \"26.0\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"BLOF1\",\r\n"
                                           + "                \"nws_lid\": \"BLOF1\",\r\n"
                                           + "                \"usgs_site_code\": \"02358700\",\r\n"
                                           + "                \"nwm_feature_id\": \"2297254\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"15.0\",\r\n"
                                           + "                \"action_stage\": \"13.0\",\r\n"
                                           + "                \"minor_stage\": \"17.0\",\r\n"
                                           + "                \"moderate_stage\": \"23.5\",\r\n"
                                           + "                \"major_stage\": \"26.0\",\r\n"
                                           + "                \"record_stage\": \"28.6\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"CEDG1\",\r\n"
                                           + "                \"nws_lid\": \"CEDG1\",\r\n"
                                           + "                \"usgs_site_code\": \"02343940\",\r\n"
                                           + "                \"nwm_feature_id\": \"2310009\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"0.0\",\r\n"
                                           + "                \"minor_stage\": \"0.0\",\r\n"
                                           + "                \"moderate_stage\": \"0.0\",\r\n"
                                           + "                \"major_stage\": \"0.0\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"SMAF1\",\r\n"
                                           + "                \"nws_lid\": \"SMAF1\",\r\n"
                                           + "                \"usgs_site_code\": \"02359170\",\r\n"
                                           + "                \"nwm_feature_id\": \"2298964\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"8.0\",\r\n"
                                           + "                \"minor_stage\": \"9.5\",\r\n"
                                           + "                \"moderate_stage\": \"11.5\",\r\n"
                                           + "                \"major_stage\": \"13.5\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"SMAF1\",\r\n"
                                           + "                \"nws_lid\": \"SMAF1\",\r\n"
                                           + "                \"usgs_site_code\": \"02359170\",\r\n"
                                           + "                \"nwm_feature_id\": \"2298964\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"8.0\",\r\n"
                                           + "                \"minor_stage\": \"9.5\",\r\n"
                                           + "                \"moderate_stage\": \"11.5\",\r\n"
                                           + "                \"major_stage\": \"13.5\",\r\n"
                                           + "                \"record_stage\": \"15.36\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"CHAF1\",\r\n"
                                           + "                \"nws_lid\": \"CHAF1\",\r\n"
                                           + "                \"usgs_site_code\": \"02358000\",\r\n"
                                           + "                \"nwm_feature_id\": \"2293124\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"56.0\",\r\n"
                                           + "                \"minor_stage\": \"0.0\",\r\n"
                                           + "                \"moderate_stage\": \"0.0\",\r\n"
                                           + "                \"major_stage\": \"0.0\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"OKFG1\",\r\n"
                                           + "                \"nws_lid\": \"OKFG1\",\r\n"
                                           + "                \"usgs_site_code\": \"02350512\",\r\n"
                                           + "                \"nwm_feature_id\": \"6447636\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"18.0\",\r\n"
                                           + "                \"minor_stage\": \"23.0\",\r\n"
                                           + "                \"moderate_stage\": \"0.0\",\r\n"
                                           + "                \"major_stage\": \"0.0\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"OKFG1\",\r\n"
                                           + "                \"nws_lid\": \"OKFG1\",\r\n"
                                           + "                \"usgs_site_code\": \"02350512\",\r\n"
                                           + "                \"nwm_feature_id\": \"6447636\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"0.0\",\r\n"
                                           + "                \"action_stage\": \"18.0\",\r\n"
                                           + "                \"minor_stage\": \"23.0\",\r\n"
                                           + "                \"moderate_stage\": \"None\",\r\n"
                                           + "                \"major_stage\": \"None\",\r\n"
                                           + "                \"record_stage\": \"40.1\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"TLPT2\",\r\n"
                                           + "                \"nws_lid\": \"TLPT2\",\r\n"
                                           + "                \"usgs_site_code\": \"07311630\",\r\n"
                                           + "                \"nwm_feature_id\": \"13525368\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-CMS\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - CMS\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"None\",\r\n"
                                           + "                \"action_stage\": \"0.0\",\r\n"
                                           + "                \"minor_stage\": \"15.0\",\r\n"
                                           + "                \"moderate_stage\": \"0.0\",\r\n"
                                           + "                \"major_stage\": \"0.0\",\r\n"
                                           + "                \"record_stage\": \"None\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        },\r\n"
                                           + "        {\r\n"
                                           + "            \"metadata\": {\r\n"
                                           + "                \"location_id\": \"TLPT2\",\r\n"
                                           + "                \"nws_lid\": \"TLPT2\",\r\n"
                                           + "                \"usgs_site_code\": \"07311630\",\r\n"
                                           + "                \"nwm_feature_id\": \"13525368\",\r\n"
                                           + "                \"id_type\": \"NWS Station\",\r\n"
                                           + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                           + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\",\r\n"
                                           + "                \"rating_source\": \"NRLDB\",\r\n"
                                           + "                \"rating_source_description\": \"NRLDB\",\r\n"
                                           + "                \"stage_unit\": \"FT\",\r\n"
                                           + "                \"flow_unit\": \"CFS\",\r\n"
                                           + "                \"rating\": {}\r\n"
                                           + "            },\r\n"
                                           + "            \"original_values\": {\r\n"
                                           + "                \"low_stage\": \"None\",\r\n"
                                           + "                \"bankfull_stage\": \"15.0\",\r\n"
                                           + "                \"action_stage\": \"None\",\r\n"
                                           + "                \"minor_stage\": \"15.0\",\r\n"
                                           + "                \"moderate_stage\": \"None\",\r\n"
                                           + "                \"major_stage\": \"None\",\r\n"
                                           + "                \"record_stage\": \"16.02\"\r\n"
                                           + "            },\r\n"
                                           + "            \"calculated_values\": {}\r\n"
                                           + "        }\r\n"
                                           + "    ]\r\n"
                                           + "}";

    // The response used is created from this URL:
    //
    // https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/
    //
    // executed on 5/20/2021 at 10:15am.
    private static final String ANOTHER_RESPONSE = "{\r\n"
                                                   + "    \"_metrics\": {\r\n"
                                                   + "        \"threshold_count\": 10,\r\n"
                                                   + "        \"total_request_time\": 0.39240050315856934\r\n"
                                                   + "    },\r\n"
                                                   + "    \"_warnings\": [],\r\n"
                                                   + "    \"_documentation\": {\r\n"
                                                   + "        \"swagger URL\": \"redacted/docs/location/v3.0/swagger/\"\r\n"
                                                   + "    },\r\n"
                                                   + "    \"deployment\": {\r\n"
                                                   + "        \"api_url\": \"https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/\",\r\n"
                                                   + "        \"stack\": \"prod\",\r\n"
                                                   + "        \"version\": \"v3.1.0\"\r\n"
                                                   + "    },\r\n"
                                                   + "    \"data_sources\": {\r\n"
                                                   + "        \"metadata_sources\": [\r\n"
                                                   + "            \"NWS data: NRLDB - Last updated: 2021-05-04 17:44:31 UTC\",\r\n"
                                                   + "            \"USGS data: USGS NWIS - Last updated: 2021-05-04 17:15:04 UTC\"\r\n"
                                                   + "        ],\r\n"
                                                   + "        \"crosswalk_datasets\": {\r\n"
                                                   + "            \"location_nwm_crosswalk_dataset\": {\r\n"
                                                   + "                \"location_nwm_crosswalk_dataset_id\": \"1.1\",\r\n"
                                                   + "                \"name\": \"Location NWM Crosswalk v1.1\",\r\n"
                                                   + "                \"description\": \"Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"nws_usgs_crosswalk_dataset\": {\r\n"
                                                   + "                \"nws_usgs_crosswalk_dataset_id\": \"1.0\",\r\n"
                                                   + "                \"name\": \"NWS Station to USGS Gages 1.0\",\r\n"
                                                   + "                \"description\": \"Authoritative 1.0 dataset mapping NWS Stations to USGS Gages\"\r\n"
                                                   + "            }\r\n"
                                                   + "        }\r\n"
                                                   + "    },\r\n"
                                                   + "    \"value_set\": [\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"PTSA1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02372250\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2323396\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"PTSA1\",\r\n"
                                                   + "                    \"id_type\": \"NWS Station\",\r\n"
                                                   + "                    \"source\": \"NRLDB\",\r\n"
                                                   + "                    \"description\": \"National Weather Service - National River Location Database\",\r\n"
                                                   + "                    \"interpolation_method\": null,\r\n"
                                                   + "                    \"interpolation_description\": null\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"PTSA1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02372250\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2323396\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"02372250\",\r\n"
                                                   + "                    \"id_type\": \"USGS Gage\",\r\n"
                                                   + "                    \"source\": \"USGS Rating Depot\",\r\n"
                                                   + "                    \"description\": \"The EXSA rating curves provided by USGS\",\r\n"
                                                   + "                    \"interpolation_method\": \"logarithmic\",\r\n"
                                                   + "                    \"interpolation_description\": \"logarithmic\"\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"MNTG1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02349605\",\r\n"
                                                   + "                \"nwm_feature_id\": \"6444276\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": -0.16,\r\n"
                                                   + "                \"bankfull\": 11.0,\r\n"
                                                   + "                \"action\": 11.0,\r\n"
                                                   + "                \"flood\": 20.0,\r\n"
                                                   + "                \"minor\": 20.0,\r\n"
                                                   + "                \"moderate\": 28.0,\r\n"
                                                   + "                \"major\": 31.0,\r\n"
                                                   + "                \"record\": 34.11\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": 11900.0,\r\n"
                                                   + "                \"flood\": 31500.0,\r\n"
                                                   + "                \"minor\": 31500.0,\r\n"
                                                   + "                \"moderate\": 77929.0,\r\n"
                                                   + "                \"major\": 105100.0,\r\n"
                                                   + "                \"record\": 136000.0\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": 557.8,\r\n"
                                                   + "                \"bankfull\": 9379.0,\r\n"
                                                   + "                \"action\": 9379.0,\r\n"
                                                   + "                \"flood\": 35331.0,\r\n"
                                                   + "                \"minor\": 35331.0,\r\n"
                                                   + "                \"moderate\": 102042.0,\r\n"
                                                   + "                \"major\": 142870.0,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"MNTG1\",\r\n"
                                                   + "                    \"id_type\": \"NWS Station\",\r\n"
                                                   + "                    \"source\": \"NRLDB\",\r\n"
                                                   + "                    \"description\": \"National Weather Service - National River Location Database\",\r\n"
                                                   + "                    \"interpolation_method\": null,\r\n"
                                                   + "                    \"interpolation_description\": null\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"MNTG1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02349605\",\r\n"
                                                   + "                \"nwm_feature_id\": \"6444276\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": -0.16,\r\n"
                                                   + "                \"bankfull\": 11.0,\r\n"
                                                   + "                \"action\": 11.0,\r\n"
                                                   + "                \"flood\": 20.0,\r\n"
                                                   + "                \"minor\": 20.0,\r\n"
                                                   + "                \"moderate\": 28.0,\r\n"
                                                   + "                \"major\": 31.0,\r\n"
                                                   + "                \"record\": 34.11\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": 11900.0,\r\n"
                                                   + "                \"flood\": 31500.0,\r\n"
                                                   + "                \"minor\": 31500.0,\r\n"
                                                   + "                \"moderate\": 77929.0,\r\n"
                                                   + "                \"major\": 105100.0,\r\n"
                                                   + "                \"record\": 136000.0\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": 554.06,\r\n"
                                                   + "                \"bankfull\": 9390.0,\r\n"
                                                   + "                \"action\": 9390.0,\r\n"
                                                   + "                \"flood\": 35329.0,\r\n"
                                                   + "                \"minor\": 35329.0,\r\n"
                                                   + "                \"moderate\": 102040.6,\r\n"
                                                   + "                \"major\": 142867.9,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"02349605\",\r\n"
                                                   + "                    \"id_type\": \"USGS Gage\",\r\n"
                                                   + "                    \"source\": \"USGS Rating Depot\",\r\n"
                                                   + "                    \"description\": \"The EXSA rating curves provided by USGS\",\r\n"
                                                   + "                    \"interpolation_method\": \"logarithmic\",\r\n"
                                                   + "                    \"interpolation_description\": \"logarithmic\"\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"BLOF1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02358700\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2297254\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": 15.0,\r\n"
                                                   + "                \"action\": 13.0,\r\n"
                                                   + "                \"flood\": 17.0,\r\n"
                                                   + "                \"minor\": 17.0,\r\n"
                                                   + "                \"moderate\": 23.5,\r\n"
                                                   + "                \"major\": 26.0,\r\n"
                                                   + "                \"record\": 28.6\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": 36900.0,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": 209000.0\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": 38633.0,\r\n"
                                                   + "                \"action\": 31313.0,\r\n"
                                                   + "                \"flood\": 48628.0,\r\n"
                                                   + "                \"minor\": 48628.0,\r\n"
                                                   + "                \"moderate\": 144077.0,\r\n"
                                                   + "                \"major\": 216266.0,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"BLOF1\",\r\n"
                                                   + "                    \"id_type\": \"NWS Station\",\r\n"
                                                   + "                    \"source\": \"NRLDB\",\r\n"
                                                   + "                    \"description\": \"National Weather Service - National River Location Database\",\r\n"
                                                   + "                    \"interpolation_method\": null,\r\n"
                                                   + "                    \"interpolation_description\": null\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"BLOF1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02358700\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2297254\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": 15.0,\r\n"
                                                   + "                \"action\": 13.0,\r\n"
                                                   + "                \"flood\": 17.0,\r\n"
                                                   + "                \"minor\": 17.0,\r\n"
                                                   + "                \"moderate\": 23.5,\r\n"
                                                   + "                \"major\": 26.0,\r\n"
                                                   + "                \"record\": 28.6\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": 36900.0,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": 209000.0\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": 36928.1,\r\n"
                                                   + "                \"action\": 30031.5,\r\n"
                                                   + "                \"flood\": 46234.6,\r\n"
                                                   + "                \"minor\": 46234.6,\r\n"
                                                   + "                \"moderate\": 133995.6,\r\n"
                                                   + "                \"major\": 205562.6,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"02358700\",\r\n"
                                                   + "                    \"id_type\": \"USGS Gage\",\r\n"
                                                   + "                    \"source\": \"USGS Rating Depot\",\r\n"
                                                   + "                    \"description\": \"The EXSA rating curves provided by USGS\",\r\n"
                                                   + "                    \"interpolation_method\": \"logarithmic\",\r\n"
                                                   + "                    \"interpolation_description\": \"logarithmic\"\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"CEDG1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02343940\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2310009\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": 14.29\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"CEDG1\",\r\n"
                                                   + "                    \"id_type\": \"NWS Station\",\r\n"
                                                   + "                    \"source\": \"NRLDB\",\r\n"
                                                   + "                    \"description\": \"National Weather Service - National River Location Database\",\r\n"
                                                   + "                    \"interpolation_method\": null,\r\n"
                                                   + "                    \"interpolation_description\": null\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"CEDG1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02343940\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2310009\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": 14.29\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": null,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"02343940\",\r\n"
                                                   + "                    \"id_type\": \"USGS Gage\",\r\n"
                                                   + "                    \"source\": \"USGS Rating Depot\",\r\n"
                                                   + "                    \"description\": \"The EXSA rating curves provided by USGS\",\r\n"
                                                   + "                    \"interpolation_method\": \"logarithmic\",\r\n"
                                                   + "                    \"interpolation_description\": \"logarithmic\"\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"SMAF1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02359170\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2298964\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": 8.0,\r\n"
                                                   + "                \"flood\": 9.5,\r\n"
                                                   + "                \"minor\": 9.5,\r\n"
                                                   + "                \"moderate\": 11.5,\r\n"
                                                   + "                \"major\": 13.5,\r\n"
                                                   + "                \"record\": 15.36\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": 179000.0\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": 45700.0,\r\n"
                                                   + "                \"flood\": 67700.0,\r\n"
                                                   + "                \"minor\": 67700.0,\r\n"
                                                   + "                \"moderate\": 107000.0,\r\n"
                                                   + "                \"major\": 159000.0,\r\n"
                                                   + "                \"record\": 221000.0,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"SMAF1\",\r\n"
                                                   + "                    \"id_type\": \"NWS Station\",\r\n"
                                                   + "                    \"source\": \"NRLDB\",\r\n"
                                                   + "                    \"description\": \"National Weather Service - National River Location Database\",\r\n"
                                                   + "                    \"interpolation_method\": null,\r\n"
                                                   + "                    \"interpolation_description\": null\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        },\r\n"
                                                   + "        {\r\n"
                                                   + "            \"metadata\": {\r\n"
                                                   + "                \"data_type\": \"NWS Stream Thresholds\",\r\n"
                                                   + "                \"nws_lid\": \"SMAF1\",\r\n"
                                                   + "                \"usgs_site_code\": \"02359170\",\r\n"
                                                   + "                \"nwm_feature_id\": \"2298964\",\r\n"
                                                   + "                \"threshold_type\": \"all (stage,flow)\",\r\n"
                                                   + "                \"stage_units\": \"FT\",\r\n"
                                                   + "                \"flow_units\": \"CFS\",\r\n"
                                                   + "                \"calc_flow_units\": \"CFS\",\r\n"
                                                   + "                \"threshold_source\": \"NWS-NRLDB\",\r\n"
                                                   + "                \"threshold_source_description\": \"National Weather Service - National River Location Database\"\r\n"
                                                   + "            },\r\n"
                                                   + "            \"stage_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": 8.0,\r\n"
                                                   + "                \"flood\": 9.5,\r\n"
                                                   + "                \"minor\": 9.5,\r\n"
                                                   + "                \"moderate\": 11.5,\r\n"
                                                   + "                \"major\": 13.5,\r\n"
                                                   + "                \"record\": 15.36\r\n"
                                                   + "            },\r\n"
                                                   + "            \"flow_values\": {\r\n"
                                                   + "                \"low\": 0.0,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": null,\r\n"
                                                   + "                \"flood\": null,\r\n"
                                                   + "                \"minor\": null,\r\n"
                                                   + "                \"moderate\": null,\r\n"
                                                   + "                \"major\": null,\r\n"
                                                   + "                \"record\": 179000.0\r\n"
                                                   + "            },\r\n"
                                                   + "            \"calc_flow_values\": {\r\n"
                                                   + "                \"low\": null,\r\n"
                                                   + "                \"bankfull\": null,\r\n"
                                                   + "                \"action\": 47355.4,\r\n"
                                                   + "                \"flood\": 69570.6,\r\n"
                                                   + "                \"minor\": 69570.6,\r\n"
                                                   + "                \"moderate\": 108505.8,\r\n"
                                                   + "                \"major\": 159463.2,\r\n"
                                                   + "                \"record\": 218963.05,\r\n"
                                                   + "                \"rating_curve\": {\r\n"
                                                   + "                    \"location_id\": \"02359170\",\r\n"
                                                   + "                    \"id_type\": \"USGS Gage\",\r\n"
                                                   + "                    \"source\": \"USGS Rating Depot\",\r\n"
                                                   + "                    \"description\": \"The EXSA rating curves provided by USGS\",\r\n"
                                                   + "                    \"interpolation_method\": \"logarithmic\",\r\n"
                                                   + "                    \"interpolation_description\": \"logarithmic\"\r\n"
                                                   + "                }\r\n"
                                                   + "            }\r\n"
                                                   + "        }\r\n"
                                                   + "    ]\r\n"
                                                   + "}\r\n"
                                                   + "";

    // The response used is created from this URL:
    //
    // https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/
    //
    // executed on 5/22/2021 in the afternoon.
    private static final String YET_ANOTHER_RESPONSE = "{\r\n"
                                                       + "    \"_metrics\": {\r\n"
                                                       + "        \"recurrence_flow_count\": 5,\r\n"
                                                       + "        \"total_request_time\": 0.30907535552978516\r\n"
                                                       + "    },\r\n"
                                                       + "    \"_warnings\": [],\r\n"
                                                       + "    \"_documentation\": {\r\n"
                                                       + "        \"swagger URL\": \"redacted/docs/location/v3.0/swagger/\"\r\n"
                                                       + "    },\r\n"
                                                       + "    \"deployment\": {\r\n"
                                                       + "        \"api_url\": \"https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/\",\r\n"
                                                       + "        \"stack\": \"prod\",\r\n"
                                                       + "        \"version\": \"v3.1.0\"\r\n"
                                                       + "    },\r\n"
                                                       + "    \"data_sources\": {\r\n"
                                                       + "        \"metadata_sources\": [\r\n"
                                                       + "            \"NWS data: NRLDB - Last updated: 2021-05-04 17:44:31 UTC\",\r\n"
                                                       + "            \"USGS data: USGS NWIS - Last updated: 2021-05-04 17:15:04 UTC\"\r\n"
                                                       + "        ],\r\n"
                                                       + "        \"crosswalk_datasets\": {\r\n"
                                                       + "            \"location_nwm_crosswalk_dataset\": {\r\n"
                                                       + "                \"location_nwm_crosswalk_dataset_id\": \"1.1\",\r\n"
                                                       + "                \"name\": \"Location NWM Crosswalk v1.1\",\r\n"
                                                       + "                \"description\": \"Created 20201106.  Source 1) NWM Routelink File v2.1   2) NHDPlus v2.1   3) GID\"\r\n"
                                                       + "            },\r\n"
                                                       + "            \"nws_usgs_crosswalk_dataset\": {\r\n"
                                                       + "                \"nws_usgs_crosswalk_dataset_id\": \"1.0\",\r\n"
                                                       + "                \"name\": \"NWS Station to USGS Gages 1.0\",\r\n"
                                                       + "                \"description\": \"Authoritative 1.0 dataset mapping NWS Stations to USGS Gages\"\r\n"
                                                       + "            }\r\n"
                                                       + "        }\r\n"
                                                       + "    },\r\n"
                                                       + "    \"value_set\": [\r\n"
                                                       + "        {\r\n"
                                                       + "            \"metadata\": {\r\n"
                                                       + "                \"data_type\": \"NWM Recurrence Flows\",\r\n"
                                                       + "                \"nws_lid\": \"BLOF1\",\r\n"
                                                       + "                \"usgs_site_code\": \"02358700\",\r\n"
                                                       + "                \"nwm_feature_id\": 2297254,\r\n"
                                                       + "                \"nwm_location_crosswalk_dataset\": \"National Water Model v2.1 Corrected\",\r\n"
                                                       + "                \"huc12\": \"031300110404\",\r\n"
                                                       + "                \"units\": \"CFS\"\r\n"
                                                       + "            },\r\n"
                                                       + "            \"values\": {\r\n"
                                                       + "                \"year_1_5\": 58864.26,\r\n"
                                                       + "                \"year_2_0\": 87362.48,\r\n"
                                                       + "                \"year_3_0\": 109539.05,\r\n"
                                                       + "                \"year_4_0\": 128454.64,\r\n"
                                                       + "                \"year_5_0\": 176406.6,\r\n"
                                                       + "                \"year_10_0\": 216831.58000000002\r\n"
                                                       + "            }\r\n"
                                                       + "        },\r\n"
                                                       + "        {\r\n"
                                                       + "            \"metadata\": {\r\n"
                                                       + "                \"data_type\": \"NWM Recurrence Flows\",\r\n"
                                                       + "                \"nws_lid\": \"SMAF1\",\r\n"
                                                       + "                \"usgs_site_code\": \"02359170\",\r\n"
                                                       + "                \"nwm_feature_id\": 2298964,\r\n"
                                                       + "                \"nwm_location_crosswalk_dataset\": \"National Water Model v2.1 Corrected\",\r\n"
                                                       + "                \"huc12\": \"031300110803\",\r\n"
                                                       + "                \"units\": \"CFS\"\r\n"
                                                       + "            },\r\n"
                                                       + "            \"values\": {\r\n"
                                                       + "                \"year_1_5\": 70810.5,\r\n"
                                                       + "                \"year_2_0\": 91810.02,\r\n"
                                                       + "                \"year_3_0\": 111115.84,\r\n"
                                                       + "                \"year_4_0\": 132437.43,\r\n"
                                                       + "                \"year_5_0\": 188351.43,\r\n"
                                                       + "                \"year_10_0\": 231709.0\r\n"
                                                       + "            }\r\n"
                                                       + "        },\r\n"
                                                       + "        {\r\n"
                                                       + "            \"metadata\": {\r\n"
                                                       + "                \"data_type\": \"NWM Recurrence Flows\",\r\n"
                                                       + "                \"nws_lid\": \"CEDG1\",\r\n"
                                                       + "                \"usgs_site_code\": \"02343940\",\r\n"
                                                       + "                \"nwm_feature_id\": 2310009,\r\n"
                                                       + "                \"nwm_location_crosswalk_dataset\": \"National Water Model v2.1 Corrected\",\r\n"
                                                       + "                \"huc12\": \"031300040707\",\r\n"
                                                       + "                \"units\": \"CFS\"\r\n"
                                                       + "            },\r\n"
                                                       + "            \"values\": {\r\n"
                                                       + "                \"year_1_5\": 933.72,\r\n"
                                                       + "                \"year_2_0\": 977.16,\r\n"
                                                       + "                \"year_3_0\": 1414.0,\r\n"
                                                       + "                \"year_4_0\": 1459.2,\r\n"
                                                       + "                \"year_5_0\": 1578.3500000000001,\r\n"
                                                       + "                \"year_10_0\": 2532.27\r\n"
                                                       + "            }\r\n"
                                                       + "        },\r\n"
                                                       + "        {\r\n"
                                                       + "            \"metadata\": {\r\n"
                                                       + "                \"data_type\": \"NWM Recurrence Flows\",\r\n"
                                                       + "                \"nws_lid\": \"PTSA1\",\r\n"
                                                       + "                \"usgs_site_code\": \"02372250\",\r\n"
                                                       + "                \"nwm_feature_id\": 2323396,\r\n"
                                                       + "                \"nwm_location_crosswalk_dataset\": \"National Water Model v2.1 Corrected\",\r\n"
                                                       + "                \"huc12\": \"031403020501\",\r\n"
                                                       + "                \"units\": \"CFS\"\r\n"
                                                       + "            },\r\n"
                                                       + "            \"values\": {\r\n"
                                                       + "                \"year_1_5\": 3165.61,\r\n"
                                                       + "                \"year_2_0\": 4641.05,\r\n"
                                                       + "                \"year_3_0\": 6824.56,\r\n"
                                                       + "                \"year_4_0\": 7885.41,\r\n"
                                                       + "                \"year_5_0\": 9001.5,\r\n"
                                                       + "                \"year_10_0\": 11249.98\r\n"
                                                       + "            }\r\n"
                                                       + "        },\r\n"
                                                       + "        {\r\n"
                                                       + "            \"metadata\": {\r\n"
                                                       + "                \"data_type\": \"NWM Recurrence Flows\",\r\n"
                                                       + "                \"nws_lid\": \"MNTG1\",\r\n"
                                                       + "                \"usgs_site_code\": \"02349605\",\r\n"
                                                       + "                \"nwm_feature_id\": 6444276,\r\n"
                                                       + "                \"nwm_location_crosswalk_dataset\": \"National Water Model v2.1 Corrected\",\r\n"
                                                       + "                \"huc12\": \"031300060207\",\r\n"
                                                       + "                \"units\": \"CFS\"\r\n"
                                                       + "            },\r\n"
                                                       + "            \"values\": {\r\n"
                                                       + "                \"year_1_5\": 14110.33,\r\n"
                                                       + "                \"year_2_0\": 18327.25,\r\n"
                                                       + "                \"year_3_0\": 28842.9,\r\n"
                                                       + "                \"year_4_0\": 30716.7,\r\n"
                                                       + "                \"year_5_0\": 32276.83,\r\n"
                                                       + "                \"year_10_0\": 43859.19\r\n"
                                                       + "            }\r\n"
                                                       + "        }\r\n"
                                                       + "    ]\r\n"
                                                       + "}\r\n"
                                                       + "";

    private static final double EPSILON = 0.00001;

    private static WrdsLocation createFeature( final String featureId, final String usgsSiteCode, final String lid )
    {
        return new WrdsLocation( featureId, usgsSiteCode, lid );
    }

    private static final WrdsLocation PTSA1 = GeneralWRDSReaderTest.createFeature( "2323396", "02372250", "PTSA1" );
    private static final WrdsLocation MNTG1 = GeneralWRDSReaderTest.createFeature( "6444276", "02349605", "MNTG1" );
    private static final WrdsLocation BLOF1 = GeneralWRDSReaderTest.createFeature( "2297254", "02358700", "BLOF1" );
    private static final WrdsLocation CEDG1 = GeneralWRDSReaderTest.createFeature( "2310009", "02343940", "CEDG1" );
    private static final WrdsLocation SMAF1 = GeneralWRDSReaderTest.createFeature( "2298964", "02359170", "SMAF1" );
    private static final WrdsLocation CHAF1 = GeneralWRDSReaderTest.createFeature( "2293124", "02358000", "CHAF1" );
    private static final WrdsLocation OKFG1 = GeneralWRDSReaderTest.createFeature( "6447636", "02350512", "OKFG1" );
    private static final WrdsLocation TLPT2 = GeneralWRDSReaderTest.createFeature( "13525368", "07311630", "TLPT2" );
    private static final WrdsLocation NUTF1 = GeneralWRDSReaderTest.createFeature( null, null, "NUTF1" );
    private static final WrdsLocation CDRA1 = GeneralWRDSReaderTest.createFeature( null, null, "CDRA1" );
    private static final WrdsLocation MUCG1 = GeneralWRDSReaderTest.createFeature( null, null, "MUCG1" );
    private static final WrdsLocation PRSG1 = GeneralWRDSReaderTest.createFeature( null, null, "PRSG1" );
    private static final WrdsLocation LSNO2 = GeneralWRDSReaderTest.createFeature( null, null, "LSNO2" );
    private static final WrdsLocation HDGA4 = GeneralWRDSReaderTest.createFeature( null, null, "HDGA4" );
    private static final WrdsLocation FAKE3 = GeneralWRDSReaderTest.createFeature( null, null, "FAKE3" );
    private static final WrdsLocation CNMP1 = GeneralWRDSReaderTest.createFeature( null, null, "CNMP1" );
    private static final WrdsLocation WLLM2 = GeneralWRDSReaderTest.createFeature( null, null, "WLLM2" );
    private static final WrdsLocation RCJD2 = GeneralWRDSReaderTest.createFeature( null, null, "RCJD2" );
    private static final WrdsLocation MUSM5 = GeneralWRDSReaderTest.createFeature( null, null, "MUSM5" );
    private static final WrdsLocation DUMM5 = GeneralWRDSReaderTest.createFeature( null, null, "DUMM5" );
    private static final WrdsLocation DMTM5 = GeneralWRDSReaderTest.createFeature( null, null, "DMTM5" );
    private static final WrdsLocation PONS2 = GeneralWRDSReaderTest.createFeature( null, null, "PONS2" );
    private static final WrdsLocation MCKG1 = GeneralWRDSReaderTest.createFeature( null, null, "MCKG1" );
    private static final WrdsLocation DSNG1 = GeneralWRDSReaderTest.createFeature( null, null, "DSNG1" );
    private static final WrdsLocation BVAW2 = GeneralWRDSReaderTest.createFeature( null, null, "BVAW2" );
    private static final WrdsLocation CNEO2 = GeneralWRDSReaderTest.createFeature( null, null, "CNEO2" );
    private static final WrdsLocation CMKT2 = GeneralWRDSReaderTest.createFeature( null, null, "CMKT2" );
    private static final WrdsLocation BDWN6 = GeneralWRDSReaderTest.createFeature( null, null, "BDWN6" );
    private static final WrdsLocation CFBN6 = GeneralWRDSReaderTest.createFeature( null, null, "CFBN6" );
    private static final WrdsLocation CCSA1 = GeneralWRDSReaderTest.createFeature( null, null, "CCSA1" );
    private static final WrdsLocation LGNN8 = GeneralWRDSReaderTest.createFeature( null, null, "LGNN8" );
    private static final WrdsLocation BCLN7 = GeneralWRDSReaderTest.createFeature( null, null, "BCLN7" );
    private static final WrdsLocation KERV2 = GeneralWRDSReaderTest.createFeature( null, null, "KERV2" );
    private static final WrdsLocation ARDS1 = GeneralWRDSReaderTest.createFeature( null, null, "ARDS1" );
    private static final WrdsLocation WINW2 = GeneralWRDSReaderTest.createFeature( null, null, "WINW2" );
    private static final WrdsLocation SRDN5 = GeneralWRDSReaderTest.createFeature( null, null, "SRDN5" );
    private static final WrdsLocation MNTN1 = GeneralWRDSReaderTest.createFeature( null, null, "MNTN1" );
    private static final WrdsLocation GNSW4 = GeneralWRDSReaderTest.createFeature( null, null, "GNSW4" );
    private static final WrdsLocation JAIO1 = GeneralWRDSReaderTest.createFeature( null, null, "JAIO1" );
    private static final WrdsLocation INCO1 = GeneralWRDSReaderTest.createFeature( null, null, "INCO1" );
    private static final WrdsLocation PRMO1 = GeneralWRDSReaderTest.createFeature( null, null, "PRMO1" );
    private static final WrdsLocation PARO1 = GeneralWRDSReaderTest.createFeature( null, null, "PARO1" );
    private static final WrdsLocation BRCO1 = GeneralWRDSReaderTest.createFeature( null, null, "BRCO1" );
    private static final WrdsLocation WRNO1 = GeneralWRDSReaderTest.createFeature( null, null, "WRNO1" );
    private static final WrdsLocation BLEO1 = GeneralWRDSReaderTest.createFeature( null, null, "BLEO1" );

    private static final List<WrdsLocation> DESIRED_FEATURES = List.of(
                                                                        PTSA1,
                                                                        MNTG1,
                                                                        BLOF1,
                                                                        CEDG1,
                                                                        SMAF1,
                                                                        CHAF1,
                                                                        OKFG1,
                                                                        TLPT2,
                                                                        NUTF1,
                                                                        CDRA1,
                                                                        MUCG1,
                                                                        PRSG1,
                                                                        LSNO2,
                                                                        HDGA4,
                                                                        FAKE3,
                                                                        CNMP1,
                                                                        WLLM2,
                                                                        RCJD2,
                                                                        MUSM5,
                                                                        DUMM5,
                                                                        DMTM5,
                                                                        PONS2,
                                                                        MCKG1,
                                                                        DSNG1,
                                                                        BVAW2,
                                                                        CNEO2,
                                                                        CMKT2,
                                                                        BDWN6,
                                                                        CFBN6,
                                                                        CCSA1,
                                                                        LGNN8,
                                                                        BCLN7,
                                                                        KERV2,
                                                                        ARDS1,
                                                                        WINW2,
                                                                        SRDN5,
                                                                        MNTN1,
                                                                        GNSW4,
                                                                        JAIO1,
                                                                        INCO1,
                                                                        PRMO1,
                                                                        PARO1,
                                                                        BRCO1,
                                                                        WRNO1,
                                                                        BLEO1 );

    private UnitMapper unitMapper;
    private static final MeasurementUnit units = MeasurementUnit.of( "CMS" );
    private SystemSettings systemSettings;

    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    @Before
    public void runBeforeEachTest()
    {
        System.setProperty( "user.timezone", "UTC" );
        this.unitMapper = Mockito.mock( UnitMapper.class );
        this.systemSettings = SystemSettings.withDefaults();
        Mockito.when( this.unitMapper.getUnitMapper( "FT" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "CFS" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getUnitMapper( "MM" ) ).thenReturn( in -> in );
        Mockito.when( this.unitMapper.getDesiredMeasurementUnitName() ).thenReturn( units.toString() );
    }

    @Test
    public void testGroupLocations()
    {
        Set<String> desiredFeatures =
                DESIRED_FEATURES.stream().map( WrdsLocation::nwsLid ).collect( Collectors.toSet() );
        Set<String> groupedLocations = GeneralWRDSReader.groupLocations( desiredFeatures );
        Assert.assertEquals( groupedLocations.size(), 3 );

        StringJoiner firstGroupBuilder = new StringJoiner( "," );
        StringJoiner secondGroupBuilder = new StringJoiner( "," );
        StringJoiner thirdGroupBuilder = new StringJoiner( "," );


        Iterator<String> desiredIterator = desiredFeatures.iterator();

        for ( int i = 0; i < GeneralWRDSReader.LOCATION_REQUEST_COUNT; i++ )
        {
            firstGroupBuilder.add( desiredIterator.next() );
        }

        for ( int i = 0; i < GeneralWRDSReader.LOCATION_REQUEST_COUNT; i++ )
        {
            secondGroupBuilder.add( desiredIterator.next() );
        }

        for ( int i = 0; i < GeneralWRDSReader.LOCATION_REQUEST_COUNT && desiredIterator.hasNext(); i++ )
        {
            thirdGroupBuilder.add( desiredIterator.next() );
        }

        String firstGroup = firstGroupBuilder.toString();
        String secondGroup = secondGroupBuilder.toString();
        String thirdGroup = thirdGroupBuilder.toString();

        Assert.assertTrue( groupedLocations.contains( firstGroup ) );
        Assert.assertTrue( groupedLocations.contains( secondGroup ) );
        Assert.assertTrue( groupedLocations.contains( thirdGroup ) );
    }

    @Test
    public void testGetResponse() throws IOException
    {
        GeneralWRDSReader reader = new GeneralWRDSReader( systemSettings );

        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( ANOTHER_RESPONSE );
            }

            byte[] responseBytes = reader.getResponse( jsonPath.toUri() );
            GeneralThresholdResponse response =
                    JSON_OBJECT_MAPPER.readValue( responseBytes, GeneralThresholdResponse.class );

            Assert.assertEquals( 10, response.getThresholds().size() );
            Iterator<GeneralThresholdDefinition> iterator = response.getThresholds().iterator();

            GeneralThresholdDefinition activeCheckedThresholds = iterator.next();

            //==== PTSA1 is first.  This is a mostly empty set of thresholds.
            //Check the PTSA1: NWS-CMS metadata
            Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThresholdSource() );
            Assert.assertEquals( "National Weather Service - National River Location Database",
                                 activeCheckedThresholds.getMetadata().getThresholdSourceDescription() );
            Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStageUnits() );
            Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlowUnits() );
            Assert.assertEquals( "NRLDB", activeCheckedThresholds.getRatingProvider() );
            Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getThresholdProvider() );
            Assert.assertEquals( "PTSA1",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getLocationId() );
            Assert.assertEquals( "NWS Station",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getIdType() );
            Assert.assertEquals( "National Weather Service - National River Location Database",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getDescription() );
            Assert.assertEquals( "NRLDB", activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getSource() );

            //Check the values with calculated flow included.
            Map<WrdsLocation, Set<ThresholdOuter>> results =
                    activeCheckedThresholds.getThresholds( WRDSThresholdType.FLOW,
                                                           Operator.GREATER,
                                                           ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                           true,
                                                           this.unitMapper );

            Set<ThresholdOuter> thresholds = results.values().iterator().next();
            Map<String, Double> expectedThresholdValues = new HashMap<>();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "low", 0.0 );

            for ( ThresholdOuter outerThreshold : thresholds )
            {
                Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     outerThreshold.getDataType() );
                Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

                Assert.assertEquals(
                                     expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                     outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            activeCheckedThresholds = iterator.next();

            //==== PTSA1 is second.  On difference: rating info.
            //Check the PTSA1: NWS-CMS metadata
            Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThresholdSource() );
            Assert.assertEquals( "National Weather Service - National River Location Database",
                                 activeCheckedThresholds.getMetadata().getThresholdSourceDescription() );
            Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStageUnits() );
            Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlowUnits() );
            Assert.assertEquals( "USGS Rating Depot", activeCheckedThresholds.getRatingProvider() );
            Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getThresholdProvider() );
            Assert.assertEquals( "02372250",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getLocationId() );
            Assert.assertEquals( "USGS Gage",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getIdType() );
            Assert.assertEquals( "The EXSA rating curves provided by USGS",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getDescription() );
            Assert.assertEquals( "USGS Rating Depot",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getSource() );

            //Check the values with calculated flow included.
            results = activeCheckedThresholds.getThresholds(
                                                             WRDSThresholdType.FLOW,
                                                             Operator.GREATER,
                                                             ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                             true,
                                                             this.unitMapper );

            thresholds = results.values().iterator().next();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "low", 0.0 );

            for ( ThresholdOuter outerThreshold : thresholds )
            {
                Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     outerThreshold.getDataType() );
                Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

                Assert.assertEquals(
                                     expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                     outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            activeCheckedThresholds = iterator.next();

            //==== MTNG1 is third.  On difference: rating info.
            //Check the PTSA1: NWS-CMS metadata
            Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThresholdSource() );
            Assert.assertEquals( "National Weather Service - National River Location Database",
                                 activeCheckedThresholds.getMetadata().getThresholdSourceDescription() );
            Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStageUnits() );
            Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlowUnits() );
            Assert.assertEquals( "NRLDB", activeCheckedThresholds.getRatingProvider() );

            //Check the values with calculated flow included.
            results = activeCheckedThresholds.getThresholds(
                                                             WRDSThresholdType.FLOW,
                                                             Operator.GREATER,
                                                             ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                             true,
                                                             this.unitMapper );

            thresholds = results.values().iterator().next();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "action", 11900.0 );
            expectedThresholdValues.put( "flood", 31500.0 );
            expectedThresholdValues.put( "minor", 31500.0 );
            expectedThresholdValues.put( "moderate", 77929.0 );
            expectedThresholdValues.put( "major", 105100.0 );
            expectedThresholdValues.put( "record", 136000.0 );

            expectedThresholdValues.put( "NRLDB low", 557.8 );
            expectedThresholdValues.put( "NRLDB bankfull", 9379.0 );
            expectedThresholdValues.put( "NRLDB action", 9379.0 );
            expectedThresholdValues.put( "NRLDB flood", 35331.0 );
            expectedThresholdValues.put( "NRLDB minor", 35331.0 );
            expectedThresholdValues.put( "NRLDB moderate", 102042.0 );
            expectedThresholdValues.put( "NRLDB major", 142870.0 );

            for ( ThresholdOuter outerThreshold : thresholds )
            {
                Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     outerThreshold.getDataType() );
                Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

                Assert.assertEquals(
                                     expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                     outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            activeCheckedThresholds = iterator.next(); //Skip the 4th. 
            activeCheckedThresholds = iterator.next(); //Frankly, I'm not even sure we need to check the 5th.

            //==== BLOF1 is fifth.  On difference: rating info.
            //Check the PTSA1: NWS-CMS metadata
            Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getMetadata().getThresholdSource() );
            Assert.assertEquals( "National Weather Service - National River Location Database",
                                 activeCheckedThresholds.getMetadata().getThresholdSourceDescription() );
            Assert.assertEquals( "FT", activeCheckedThresholds.getMetadata().getStageUnits() );
            Assert.assertEquals( "CFS", activeCheckedThresholds.getMetadata().getFlowUnits() );
            Assert.assertEquals( "NRLDB", activeCheckedThresholds.getRatingProvider() );
            Assert.assertEquals( "NWS-NRLDB", activeCheckedThresholds.getThresholdProvider() );
            Assert.assertEquals( "BLOF1", activeCheckedThresholds.getMetadata().getNwsLid() );
            Assert.assertEquals( "all (stage,flow)", activeCheckedThresholds.getMetadata().getThresholdType() );
            Assert.assertEquals( "BLOF1",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getLocationId() );
            Assert.assertEquals( "NWS Station",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getIdType() );
            Assert.assertEquals( "National Weather Service - National River Location Database",
                                 activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getDescription() );
            Assert.assertEquals( "NRLDB", activeCheckedThresholds.getCalcFlowValues().getRatingCurve().getSource() );

            //Stage
            results = activeCheckedThresholds.getThresholds(
                                                             WRDSThresholdType.STAGE,
                                                             Operator.GREATER,
                                                             ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                             true,
                                                             this.unitMapper );

            thresholds = results.values().iterator().next();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "bankfull", 15.0 );
            expectedThresholdValues.put( "action", 13.0 );
            expectedThresholdValues.put( "flood", 17.0 );
            expectedThresholdValues.put( "minor", 17.0 );
            expectedThresholdValues.put( "moderate", 23.5 );
            expectedThresholdValues.put( "major", 26.0 );
            expectedThresholdValues.put( "record", 28.6 );

            for ( ThresholdOuter outerThreshold : thresholds )
            {
                Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     outerThreshold.getDataType() );
                Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

                Assert.assertEquals(
                                     expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                     outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            //Check the values with calculated flow included.
            results = activeCheckedThresholds.getThresholds(
                                                             WRDSThresholdType.FLOW,
                                                             Operator.GREATER,
                                                             ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                             true,
                                                             this.unitMapper );

            thresholds = results.values().iterator().next();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "flood", 36900.0 );
            expectedThresholdValues.put( "record", 209000.0 );

            expectedThresholdValues.put( "NRLDB bankfull", 38633.0 );
            expectedThresholdValues.put( "NRLDB action", 31313.0 );
            expectedThresholdValues.put( "NRLDB flood", 48628.0 );
            expectedThresholdValues.put( "NRLDB minor", 48628.0 );
            expectedThresholdValues.put( "NRLDB moderate", 144077.0 );
            expectedThresholdValues.put( "NRLDB major", 216266.0 );

            for ( ThresholdOuter outerThreshold : thresholds )
            {
                Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     outerThreshold.getDataType() );
                Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

                Assert.assertEquals(
                                     expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                     outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            //Check the values with raw flow only.
            results = activeCheckedThresholds.getThresholds(
                                                             WRDSThresholdType.FLOW,
                                                             Operator.GREATER,
                                                             ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                             false,
                                                             this.unitMapper );

            thresholds = results.values().iterator().next();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "flood", 36900.0 );
            ;
            expectedThresholdValues.put( "record", 209000.0 );

            for ( ThresholdOuter outerThreshold : thresholds )
            {
                Assert.assertEquals( ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     outerThreshold.getDataType() );
                Assert.assertEquals( Operator.GREATER, outerThreshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( outerThreshold.getThreshold().getName() ) );

                Assert.assertEquals(
                                     expectedThresholdValues.get( outerThreshold.getThreshold().getName() ),
                                     outerThreshold.getThreshold().getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            //I believe additional testing of the remaining thresholds is unnecessary.

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }

    @Test
    public void testReadThresholds() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( ANOTHER_RESPONSE );
            }

            ThresholdsConfig normalThresholdConfig = new ThresholdsConfig( ThresholdType.VALUE,
                                                                           ThresholdDataType.LEFT,
                                                                           new ThresholdsConfig.Source( jsonPath.toUri(),
                                                                                                        ThresholdFormat.WRDS,
                                                                                                        null,
                                                                                                        null,
                                                                                                        "NWS-NRLDB",
                                                                                                        null, //null ratings provider.
                                                                                                        "stage",
                                                                                                        LeftOrRightOrBaseline.LEFT ),
                                                                           ThresholdOperator.GREATER_THAN );

            Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds( systemSettings,
                                                                                                      normalThresholdConfig,
                                                                                                      this.unitMapper,
                                                                                                      FeatureDimension.NWS_LID,
                                                                                                      DESIRED_FEATURES.stream()
                                                                                                                      .map( WrdsLocation::nwsLid )
                                                                                                                      .collect( Collectors.toSet() ) );

            Assert.assertTrue( readThresholds.containsKey( PTSA1 ) );
            Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
            Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
            Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
            Assert.assertTrue( readThresholds.containsKey( CEDG1 ) );

            //The two low thresholds available are identical in both label and value, so only one is included.
            Set<ThresholdOuter> ptsa1Thresholds = readThresholds.get( PTSA1 );
            Assert.assertEquals( 1, ptsa1Thresholds.size() );

            Set<ThresholdOuter> blof1Thresholds = readThresholds.get( BLOF1 );
            Assert.assertEquals( 7, blof1Thresholds.size() );

            boolean hasLow = false;
            boolean hasBankfull = false;
            boolean hasAction = false;
            boolean hasMinor = false;
            boolean hasModerate = false;
            boolean hasMajor = false;
            boolean hasRecord = false;

            List<String> properThresholds = List.of(
                                                     "bankfull",
                                                     "action",
                                                     "flood",
                                                     "minor",
                                                     "moderate",
                                                     "major",
                                                     "record" );

            for ( ThresholdOuter thresholdOuter : blof1Thresholds )
            {
                String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull":
                        hasBankfull = true;
                        Assert.assertEquals(
                                             15.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "action":
                        hasAction = true;
                        Assert.assertEquals(
                                             13.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "flood":
                        hasMinor = true;
                        Assert.assertEquals(
                                             17.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "minor":
                        hasMinor = true;
                        Assert.assertEquals(
                                             17.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "moderate":
                        hasModerate = true;
                        Assert.assertEquals(
                                             23.5,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "major":
                        hasMajor = true;
                        Assert.assertEquals(
                                             26.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "record":
                        hasRecord = true;
                        Assert.assertEquals(
                                             28.6,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                }
            }

            Assert.assertFalse( hasLow );
            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasModerate );
            Assert.assertTrue( hasMajor );
            Assert.assertTrue( hasRecord );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }

    @Test
    public void testReadRecurrenceFlows() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( YET_ANOTHER_RESPONSE );
            }

            ThresholdsConfig normalRecurrenceConfig = new ThresholdsConfig(
                                                                            ThresholdType.VALUE,
                                                                            ThresholdDataType.LEFT,
                                                                            new ThresholdsConfig.Source( jsonPath.toUri(),
                                                                                                         ThresholdFormat.WRDS,
                                                                                                         null,
                                                                                                         null,
                                                                                                         null,
                                                                                                         null, //null ratings provider.
                                                                                                         null,
                                                                                                         LeftOrRightOrBaseline.LEFT ),
                                                                            ThresholdOperator.GREATER_THAN );

            Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds( systemSettings,
                                                                                                      normalRecurrenceConfig,
                                                                                                      this.unitMapper,
                                                                                                      FeatureDimension.NWS_LID,
                                                                                                      DESIRED_FEATURES.stream()
                                                                                                                      .map( WrdsLocation::nwsLid )
                                                                                                                      .collect( Collectors.toSet() ) );

            Assert.assertTrue( readThresholds.containsKey( PTSA1 ) );
            Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
            Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
            Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
            Assert.assertTrue( readThresholds.containsKey( CEDG1 ) );


            Set<ThresholdOuter> blof1Thresholds = readThresholds.get( BLOF1 );
            Assert.assertEquals( 6, blof1Thresholds.size() );

            boolean has1_5 = false;
            boolean has2_0 = false;
            boolean has3_0 = false;
            boolean has4_0 = false;
            boolean has5_0 = false;
            boolean has10_0 = false;

            List<String> properThresholds = List.of(
                                                     "year_1_5",
                                                     "year_2_0",
                                                     "year_3_0",
                                                     "year_4_0",
                                                     "year_5_0",
                                                     "year_10_0" );

            for ( ThresholdOuter thresholdOuter : blof1Thresholds )
            {
                String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "year_1_5":
                        has1_5 = true;
                        Assert.assertEquals(
                                             58864.26,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "year_2_0":
                        has2_0 = true;
                        Assert.assertEquals(
                                             87362.48,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "year_3_0":
                        has3_0 = true;
                        Assert.assertEquals(
                                             109539.05,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "year_4_0":
                        has4_0 = true;
                        Assert.assertEquals(
                                             128454.64,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "year_5_0":
                        has5_0 = true;
                        Assert.assertEquals(
                                             176406.6,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "year_10_0":
                        has10_0 = true;
                        Assert.assertEquals(
                                             216831.58000000002,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                }
            }

            Assert.assertTrue( has1_5 );
            Assert.assertTrue( has2_0 );
            Assert.assertTrue( has3_0 );
            Assert.assertTrue( has4_0 );
            Assert.assertTrue( has5_0 );
            Assert.assertTrue( has10_0 );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }

    @Test
    public void testReadOldThresholds() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new csv file to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( RESPONSE );
            }

            ThresholdsConfig oldNormalThresholdConfig = new ThresholdsConfig(
                                                                              ThresholdType.VALUE,
                                                                              ThresholdDataType.LEFT,
                                                                              new ThresholdsConfig.Source( jsonPath.toUri(),
                                                                                                           ThresholdFormat.WRDS,
                                                                                                           null,
                                                                                                           null,
                                                                                                           "NWS-NRLDB",
                                                                                                           null,
                                                                                                           "stage",
                                                                                                           LeftOrRightOrBaseline.LEFT ),
                                                                              ThresholdOperator.GREATER_THAN );

            Map<WrdsLocation, Set<ThresholdOuter>> readThresholds = GeneralWRDSReader.readThresholds( systemSettings,
                                                                                                      oldNormalThresholdConfig,
                                                                                                      this.unitMapper,
                                                                                                      FeatureDimension.NWS_LID,
                                                                                                      DESIRED_FEATURES.stream()
                                                                                                                      .map( WrdsLocation::nwsLid )
                                                                                                                      .collect( Collectors.toSet() ) );

            Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
            Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
            Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
            Assert.assertTrue( readThresholds.containsKey( OKFG1 ) );
            Assert.assertTrue( readThresholds.containsKey( TLPT2 ) );

            Set<ThresholdOuter> mntg1Thresholds = readThresholds.get( MNTG1 );

            Assert.assertEquals( 6, mntg1Thresholds.size() );

            boolean hasLow = false;
            boolean hasBankfull = false;
            boolean hasAction = false;
            boolean hasMinor = false;
            boolean hasModerate = false;
            boolean hasMajor = false;
            boolean hasRecord = false;

            List<String> properThresholds = List.of(
                                                     "bankfull",
                                                     "action",
                                                     "minor",
                                                     "moderate",
                                                     "major",
                                                     "record" );

            for ( ThresholdOuter thresholdOuter : mntg1Thresholds )
            {
                String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull":
                        hasBankfull = true;
                        Assert.assertEquals(
                                             11.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "action":
                        hasAction = true;
                        Assert.assertEquals(
                                             11.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "minor":
                        hasMinor = true;
                        Assert.assertEquals(
                                             20.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "moderate":
                        hasModerate = true;
                        Assert.assertEquals(
                                             28.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "major":
                        hasMajor = true;
                        Assert.assertEquals(
                                             31.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "record":
                        hasRecord = true;
                        Assert.assertEquals(
                                             34.11,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                }
            }

            Assert.assertFalse( hasLow );
            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasModerate );
            Assert.assertTrue( hasMajor );
            Assert.assertTrue( hasRecord );

            Set<ThresholdOuter> blof1Thresholds = readThresholds.get( BLOF1 );

            Assert.assertEquals( 6, blof1Thresholds.size() );

            hasLow = false;
            hasBankfull = false;
            hasAction = false;
            hasMinor = false;
            hasModerate = false;
            hasMajor = false;
            hasRecord = false;

            properThresholds = List.of(
                                        "bankfull",
                                        "action",
                                        "minor",
                                        "moderate",
                                        "major",
                                        "record" );

            for ( ThresholdOuter thresholdOuter : blof1Thresholds )
            {
                String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull":
                        hasBankfull = true;
                        Assert.assertEquals(
                                             15.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "action":
                        hasAction = true;
                        Assert.assertEquals(
                                             13.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "minor":
                        hasMinor = true;
                        Assert.assertEquals(
                                             17.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "moderate":
                        hasModerate = true;
                        Assert.assertEquals(
                                             23.5,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "major":
                        hasMajor = true;
                        Assert.assertEquals(
                                             26.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "record":
                        hasRecord = true;
                        Assert.assertEquals(
                                             28.6,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                }
            }

            Assert.assertFalse( hasLow );
            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasModerate );
            Assert.assertTrue( hasMajor );
            Assert.assertTrue( hasRecord );

            Set<ThresholdOuter> smaf1Thresholds = readThresholds.get( SMAF1 );

            Assert.assertEquals( 5, smaf1Thresholds.size() );

            hasLow = false;
            hasBankfull = false;
            hasAction = false;
            hasMinor = false;
            hasModerate = false;
            hasMajor = false;
            hasRecord = false;

            properThresholds = List.of(
                                        "action",
                                        "minor",
                                        "moderate",
                                        "major",
                                        "record" );

            for ( ThresholdOuter thresholdOuter : smaf1Thresholds )
            {
                String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "action":
                        hasAction = true;
                        Assert.assertEquals(
                                             8.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "minor":
                        hasMinor = true;
                        Assert.assertEquals(
                                             9.5,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "moderate":
                        hasModerate = true;
                        Assert.assertEquals(
                                             11.5,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "major":
                        hasMajor = true;
                        Assert.assertEquals(
                                             13.5,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "record":
                        hasRecord = true;
                        Assert.assertEquals(
                                             15.36,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                }
            }

            Assert.assertFalse( hasLow );
            Assert.assertFalse( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasModerate );
            Assert.assertTrue( hasMajor );
            Assert.assertTrue( hasRecord );

            Set<ThresholdOuter> okfg1Thresholds = readThresholds.get( OKFG1 );

            Assert.assertEquals( 4, okfg1Thresholds.size() );

            hasLow = false;
            hasBankfull = false;
            hasAction = false;
            hasMinor = false;
            hasModerate = false;
            hasMajor = false;
            hasRecord = false;

            properThresholds = List.of(
                                        "bankfull",
                                        "action",
                                        "minor",
                                        "record" );

            for ( ThresholdOuter thresholdOuter : okfg1Thresholds )
            {
                String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull":
                        hasBankfull = true;
                        Assert.assertEquals(
                                             0.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "action":
                        hasAction = true;
                        Assert.assertEquals(
                                             18.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "minor":
                        hasMinor = true;
                        Assert.assertEquals(
                                             23.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "record":
                        hasRecord = true;
                        Assert.assertEquals(
                                             40.1,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                }
            }

            Assert.assertFalse( hasLow );
            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertFalse( hasModerate );
            Assert.assertFalse( hasMajor );
            Assert.assertTrue( hasRecord );

            Set<ThresholdOuter> tlpt2Thresholds = readThresholds.get( TLPT2 );

            Assert.assertEquals( 3, tlpt2Thresholds.size() );

            hasLow = false;
            hasBankfull = false;
            hasAction = false;
            hasMinor = false;
            hasModerate = false;
            hasMajor = false;
            hasRecord = false;

            properThresholds = List.of(
                                        "bankfull",
                                        "minor",
                                        "record" );

            for ( ThresholdOuter thresholdOuter : tlpt2Thresholds )
            {
                String thresholdName = thresholdOuter.getThreshold().getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull":
                        hasBankfull = true;
                        Assert.assertEquals(
                                             15.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "minor":
                        hasMinor = true;
                        Assert.assertEquals(
                                             15.0,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                    case "record":
                        hasRecord = true;
                        Assert.assertEquals(
                                             16.02,
                                             thresholdOuter.getThreshold().getLeftThresholdValue().getValue(),
                                             EPSILON );
                        break;
                }
            }

            Assert.assertFalse( hasLow );
            Assert.assertTrue( hasBankfull );
            Assert.assertFalse( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertFalse( hasModerate );
            Assert.assertFalse( hasMajor );
            Assert.assertTrue( hasRecord );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }
}
