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
import wres.io.geography.wrds.WrdsLocation;
import wres.datamodel.units.UnitMapper;
import wres.io.thresholds.wrds.v3.GeneralThresholdDefinition;
import wres.io.thresholds.wrds.v3.GeneralThresholdResponse;
import wres.statistics.generated.Threshold;
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
    private static final String RESPONSE = """
            {\r
                "_documentation": "redacted/docs/stage/location/swagger/",\r
                "_metrics": {\r
                    "threshold_count": 13,\r
                    "total_request_time": 0.256730318069458\r
                },\r
                "thresholds": [\r
                    {\r
                        "metadata": {\r
                            "location_id": "PTSA1",\r
                            "nws_lid": "PTSA1",\r
                            "usgs_site_code": "02372250",\r
                            "nwm_feature_id": "2323396",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "0.0",\r
                            "minor_stage": "0.0",\r
                            "moderate_stage": "0.0",\r
                            "major_stage": "0.0",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "MNTG1",\r
                            "nws_lid": "MNTG1",\r
                            "usgs_site_code": "02349605",\r
                            "nwm_feature_id": "6444276",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "11.0",\r
                            "minor_stage": "20.0",\r
                            "moderate_stage": "28.0",\r
                            "major_stage": "31.0",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "MNTG1",\r
                            "nws_lid": "MNTG1",\r
                            "usgs_site_code": "02349605",\r
                            "nwm_feature_id": "6444276",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "11.0",\r
                            "action_stage": "11.0",\r
                            "minor_stage": "20.0",\r
                            "moderate_stage": "28.0",\r
                            "major_stage": "31.0",\r
                            "record_stage": "34.11"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "BLOF1",\r
                            "nws_lid": "BLOF1",\r
                            "usgs_site_code": "02358700",\r
                            "nwm_feature_id": "2297254",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "13.0",\r
                            "minor_stage": "17.0",\r
                            "moderate_stage": "23.5",\r
                            "major_stage": "26.0",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "BLOF1",\r
                            "nws_lid": "BLOF1",\r
                            "usgs_site_code": "02358700",\r
                            "nwm_feature_id": "2297254",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "15.0",\r
                            "action_stage": "13.0",\r
                            "minor_stage": "17.0",\r
                            "moderate_stage": "23.5",\r
                            "major_stage": "26.0",\r
                            "record_stage": "28.6"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "CEDG1",\r
                            "nws_lid": "CEDG1",\r
                            "usgs_site_code": "02343940",\r
                            "nwm_feature_id": "2310009",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "0.0",\r
                            "minor_stage": "0.0",\r
                            "moderate_stage": "0.0",\r
                            "major_stage": "0.0",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "SMAF1",\r
                            "nws_lid": "SMAF1",\r
                            "usgs_site_code": "02359170",\r
                            "nwm_feature_id": "2298964",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "8.0",\r
                            "minor_stage": "9.5",\r
                            "moderate_stage": "11.5",\r
                            "major_stage": "13.5",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "SMAF1",\r
                            "nws_lid": "SMAF1",\r
                            "usgs_site_code": "02359170",\r
                            "nwm_feature_id": "2298964",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "8.0",\r
                            "minor_stage": "9.5",\r
                            "moderate_stage": "11.5",\r
                            "major_stage": "13.5",\r
                            "record_stage": "15.36"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "CHAF1",\r
                            "nws_lid": "CHAF1",\r
                            "usgs_site_code": "02358000",\r
                            "nwm_feature_id": "2293124",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "56.0",\r
                            "minor_stage": "0.0",\r
                            "moderate_stage": "0.0",\r
                            "major_stage": "0.0",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "OKFG1",\r
                            "nws_lid": "OKFG1",\r
                            "usgs_site_code": "02350512",\r
                            "nwm_feature_id": "6447636",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "18.0",\r
                            "minor_stage": "23.0",\r
                            "moderate_stage": "0.0",\r
                            "major_stage": "0.0",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "OKFG1",\r
                            "nws_lid": "OKFG1",\r
                            "usgs_site_code": "02350512",\r
                            "nwm_feature_id": "6447636",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "0.0",\r
                            "action_stage": "18.0",\r
                            "minor_stage": "23.0",\r
                            "moderate_stage": "None",\r
                            "major_stage": "None",\r
                            "record_stage": "40.1"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "TLPT2",\r
                            "nws_lid": "TLPT2",\r
                            "usgs_site_code": "07311630",\r
                            "nwm_feature_id": "13525368",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-CMS",\r
                            "threshold_source_description": "National Weather Service - CMS",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "None",\r
                            "action_stage": "0.0",\r
                            "minor_stage": "15.0",\r
                            "moderate_stage": "0.0",\r
                            "major_stage": "0.0",\r
                            "record_stage": "None"\r
                        },\r
                        "calculated_values": {}\r
                    },\r
                    {\r
                        "metadata": {\r
                            "location_id": "TLPT2",\r
                            "nws_lid": "TLPT2",\r
                            "usgs_site_code": "07311630",\r
                            "nwm_feature_id": "13525368",\r
                            "id_type": "NWS Station",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database",\r
                            "rating_source": "NRLDB",\r
                            "rating_source_description": "NRLDB",\r
                            "stage_unit": "FT",\r
                            "flow_unit": "CFS",\r
                            "rating": {}\r
                        },\r
                        "original_values": {\r
                            "low_stage": "None",\r
                            "bankfull_stage": "15.0",\r
                            "action_stage": "None",\r
                            "minor_stage": "15.0",\r
                            "moderate_stage": "None",\r
                            "major_stage": "None",\r
                            "record_stage": "16.02"\r
                        },\r
                        "calculated_values": {}\r
                    }\r
                ]\r
            }""";

    // The response used is created from this URL:
    //
    // https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/
    //
    // executed on 5/20/2021 at 10:15am.
    private static final String ANOTHER_RESPONSE = """
            {\r
                "_metrics": {\r
                    "threshold_count": 10,\r
                    "total_request_time": 0.39240050315856934\r
                },\r
                "_warnings": [],\r
                "_documentation": {\r
                    "swagger URL": "redacted/docs/location/v3.0/swagger/"\r
                },\r
                "deployment": {\r
                    "api_url": "https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/",\r
                    "stack": "prod",\r
                    "version": "v3.1.0"\r
                },\r
                "data_sources": {\r
                    "metadata_sources": [\r
                        "NWS data: NRLDB - Last updated: 2021-05-04 17:44:31 UTC",\r
                        "USGS data: USGS NWIS - Last updated: 2021-05-04 17:15:04 UTC"\r
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
                "value_set": [\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "PTSA1",\r
                            "usgs_site_code": "02372250",\r
                            "nwm_feature_id": "2323396",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null\r
                        },\r
                        "flow_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "PTSA1",\r
                                "id_type": "NWS Station",\r
                                "source": "NRLDB",\r
                                "description": "National Weather Service - National River Location Database",\r
                                "interpolation_method": null,\r
                                "interpolation_description": null\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "PTSA1",\r
                            "usgs_site_code": "02372250",\r
                            "nwm_feature_id": "2323396",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null\r
                        },\r
                        "flow_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "02372250",\r
                                "id_type": "USGS Gage",\r
                                "source": "USGS Rating Depot",\r
                                "description": "The EXSA rating curves provided by USGS",\r
                                "interpolation_method": "logarithmic",\r
                                "interpolation_description": "logarithmic"\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "MNTG1",\r
                            "usgs_site_code": "02349605",\r
                            "nwm_feature_id": "6444276",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": -0.16,\r
                            "bankfull": 11.0,\r
                            "action": 11.0,\r
                            "flood": 20.0,\r
                            "minor": 20.0,\r
                            "moderate": 28.0,\r
                            "major": 31.0,\r
                            "record": 34.11\r
                        },\r
                        "flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": 11900.0,\r
                            "flood": 31500.0,\r
                            "minor": 31500.0,\r
                            "moderate": 77929.0,\r
                            "major": 105100.0,\r
                            "record": 136000.0\r
                        },\r
                        "calc_flow_values": {\r
                            "low": 557.8,\r
                            "bankfull": 9379.0,\r
                            "action": 9379.0,\r
                            "flood": 35331.0,\r
                            "minor": 35331.0,\r
                            "moderate": 102042.0,\r
                            "major": 142870.0,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "MNTG1",\r
                                "id_type": "NWS Station",\r
                                "source": "NRLDB",\r
                                "description": "National Weather Service - National River Location Database",\r
                                "interpolation_method": null,\r
                                "interpolation_description": null\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "MNTG1",\r
                            "usgs_site_code": "02349605",\r
                            "nwm_feature_id": "6444276",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": -0.16,\r
                            "bankfull": 11.0,\r
                            "action": 11.0,\r
                            "flood": 20.0,\r
                            "minor": 20.0,\r
                            "moderate": 28.0,\r
                            "major": 31.0,\r
                            "record": 34.11\r
                        },\r
                        "flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": 11900.0,\r
                            "flood": 31500.0,\r
                            "minor": 31500.0,\r
                            "moderate": 77929.0,\r
                            "major": 105100.0,\r
                            "record": 136000.0\r
                        },\r
                        "calc_flow_values": {\r
                            "low": 554.06,\r
                            "bankfull": 9390.0,\r
                            "action": 9390.0,\r
                            "flood": 35329.0,\r
                            "minor": 35329.0,\r
                            "moderate": 102040.6,\r
                            "major": 142867.9,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "02349605",\r
                                "id_type": "USGS Gage",\r
                                "source": "USGS Rating Depot",\r
                                "description": "The EXSA rating curves provided by USGS",\r
                                "interpolation_method": "logarithmic",\r
                                "interpolation_description": "logarithmic"\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "BLOF1",\r
                            "usgs_site_code": "02358700",\r
                            "nwm_feature_id": "2297254",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": null,\r
                            "bankfull": 15.0,\r
                            "action": 13.0,\r
                            "flood": 17.0,\r
                            "minor": 17.0,\r
                            "moderate": 23.5,\r
                            "major": 26.0,\r
                            "record": 28.6\r
                        },\r
                        "flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": 36900.0,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": 209000.0\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": 38633.0,\r
                            "action": 31313.0,\r
                            "flood": 48628.0,\r
                            "minor": 48628.0,\r
                            "moderate": 144077.0,\r
                            "major": 216266.0,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "BLOF1",\r
                                "id_type": "NWS Station",\r
                                "source": "NRLDB",\r
                                "description": "National Weather Service - National River Location Database",\r
                                "interpolation_method": null,\r
                                "interpolation_description": null\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "BLOF1",\r
                            "usgs_site_code": "02358700",\r
                            "nwm_feature_id": "2297254",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": null,\r
                            "bankfull": 15.0,\r
                            "action": 13.0,\r
                            "flood": 17.0,\r
                            "minor": 17.0,\r
                            "moderate": 23.5,\r
                            "major": 26.0,\r
                            "record": 28.6\r
                        },\r
                        "flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": 36900.0,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": 209000.0\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": 36928.1,\r
                            "action": 30031.5,\r
                            "flood": 46234.6,\r
                            "minor": 46234.6,\r
                            "moderate": 133995.6,\r
                            "major": 205562.6,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "02358700",\r
                                "id_type": "USGS Gage",\r
                                "source": "USGS Rating Depot",\r
                                "description": "The EXSA rating curves provided by USGS",\r
                                "interpolation_method": "logarithmic",\r
                                "interpolation_description": "logarithmic"\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "CEDG1",\r
                            "usgs_site_code": "02343940",\r
                            "nwm_feature_id": "2310009",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": 14.29\r
                        },\r
                        "flow_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "CEDG1",\r
                                "id_type": "NWS Station",\r
                                "source": "NRLDB",\r
                                "description": "National Weather Service - National River Location Database",\r
                                "interpolation_method": null,\r
                                "interpolation_description": null\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "CEDG1",\r
                            "usgs_site_code": "02343940",\r
                            "nwm_feature_id": "2310009",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": 14.29\r
                        },\r
                        "flow_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": null,\r
                            "rating_curve": {\r
                                "location_id": "02343940",\r
                                "id_type": "USGS Gage",\r
                                "source": "USGS Rating Depot",\r
                                "description": "The EXSA rating curves provided by USGS",\r
                                "interpolation_method": "logarithmic",\r
                                "interpolation_description": "logarithmic"\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "SMAF1",\r
                            "usgs_site_code": "02359170",\r
                            "nwm_feature_id": "2298964",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": 8.0,\r
                            "flood": 9.5,\r
                            "minor": 9.5,\r
                            "moderate": 11.5,\r
                            "major": 13.5,\r
                            "record": 15.36\r
                        },\r
                        "flow_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": 179000.0\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": 45700.0,\r
                            "flood": 67700.0,\r
                            "minor": 67700.0,\r
                            "moderate": 107000.0,\r
                            "major": 159000.0,\r
                            "record": 221000.0,\r
                            "rating_curve": {\r
                                "location_id": "SMAF1",\r
                                "id_type": "NWS Station",\r
                                "source": "NRLDB",\r
                                "description": "National Weather Service - National River Location Database",\r
                                "interpolation_method": null,\r
                                "interpolation_description": null\r
                            }\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWS Stream Thresholds",\r
                            "nws_lid": "SMAF1",\r
                            "usgs_site_code": "02359170",\r
                            "nwm_feature_id": "2298964",\r
                            "threshold_type": "all (stage,flow)",\r
                            "stage_units": "FT",\r
                            "flow_units": "CFS",\r
                            "calc_flow_units": "CFS",\r
                            "threshold_source": "NWS-NRLDB",\r
                            "threshold_source_description": "National Weather Service - National River Location Database"\r
                        },\r
                        "stage_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": 8.0,\r
                            "flood": 9.5,\r
                            "minor": 9.5,\r
                            "moderate": 11.5,\r
                            "major": 13.5,\r
                            "record": 15.36\r
                        },\r
                        "flow_values": {\r
                            "low": 0.0,\r
                            "bankfull": null,\r
                            "action": null,\r
                            "flood": null,\r
                            "minor": null,\r
                            "moderate": null,\r
                            "major": null,\r
                            "record": 179000.0\r
                        },\r
                        "calc_flow_values": {\r
                            "low": null,\r
                            "bankfull": null,\r
                            "action": 47355.4,\r
                            "flood": 69570.6,\r
                            "minor": 69570.6,\r
                            "moderate": 108505.8,\r
                            "major": 159463.2,\r
                            "record": 218963.05,\r
                            "rating_curve": {\r
                                "location_id": "02359170",\r
                                "id_type": "USGS Gage",\r
                                "source": "USGS Rating Depot",\r
                                "description": "The EXSA rating curves provided by USGS",\r
                                "interpolation_method": "logarithmic",\r
                                "interpolation_description": "logarithmic"\r
                            }\r
                        }\r
                    }\r
                ]\r
            }\r
            """;

    // The response used is created from this URL:
    //
    // https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/
    //
    // executed on 5/22/2021 in the afternoon.
    private static final String YET_ANOTHER_RESPONSE = """
            {\r
                "_metrics": {\r
                    "recurrence_flow_count": 5,\r
                    "total_request_time": 0.30907535552978516\r
                },\r
                "_warnings": [],\r
                "_documentation": {\r
                    "swagger URL": "redacted/docs/location/v3.0/swagger/"\r
                },\r
                "deployment": {\r
                    "api_url": "https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/",\r
                    "stack": "prod",\r
                    "version": "v3.1.0"\r
                },\r
                "data_sources": {\r
                    "metadata_sources": [\r
                        "NWS data: NRLDB - Last updated: 2021-05-04 17:44:31 UTC",\r
                        "USGS data: USGS NWIS - Last updated: 2021-05-04 17:15:04 UTC"\r
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
                "value_set": [\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWM Recurrence Flows",\r
                            "nws_lid": "BLOF1",\r
                            "usgs_site_code": "02358700",\r
                            "nwm_feature_id": 2297254,\r
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",\r
                            "huc12": "031300110404",\r
                            "units": "CFS"\r
                        },\r
                        "values": {\r
                            "year_1_5": 58864.26,\r
                            "year_2_0": 87362.48,\r
                            "year_3_0": 109539.05,\r
                            "year_4_0": 128454.64,\r
                            "year_5_0": 176406.6,\r
                            "year_10_0": 216831.58000000002\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWM Recurrence Flows",\r
                            "nws_lid": "SMAF1",\r
                            "usgs_site_code": "02359170",\r
                            "nwm_feature_id": 2298964,\r
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",\r
                            "huc12": "031300110803",\r
                            "units": "CFS"\r
                        },\r
                        "values": {\r
                            "year_1_5": 70810.5,\r
                            "year_2_0": 91810.02,\r
                            "year_3_0": 111115.84,\r
                            "year_4_0": 132437.43,\r
                            "year_5_0": 188351.43,\r
                            "year_10_0": 231709.0\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWM Recurrence Flows",\r
                            "nws_lid": "CEDG1",\r
                            "usgs_site_code": "02343940",\r
                            "nwm_feature_id": 2310009,\r
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",\r
                            "huc12": "031300040707",\r
                            "units": "CFS"\r
                        },\r
                        "values": {\r
                            "year_1_5": 933.72,\r
                            "year_2_0": 977.16,\r
                            "year_3_0": 1414.0,\r
                            "year_4_0": 1459.2,\r
                            "year_5_0": 1578.3500000000001,\r
                            "year_10_0": 2532.27\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWM Recurrence Flows",\r
                            "nws_lid": "PTSA1",\r
                            "usgs_site_code": "02372250",\r
                            "nwm_feature_id": 2323396,\r
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",\r
                            "huc12": "031403020501",\r
                            "units": "CFS"\r
                        },\r
                        "values": {\r
                            "year_1_5": 3165.61,\r
                            "year_2_0": 4641.05,\r
                            "year_3_0": 6824.56,\r
                            "year_4_0": 7885.41,\r
                            "year_5_0": 9001.5,\r
                            "year_10_0": 11249.98\r
                        }\r
                    },\r
                    {\r
                        "metadata": {\r
                            "data_type": "NWM Recurrence Flows",\r
                            "nws_lid": "MNTG1",\r
                            "usgs_site_code": "02349605",\r
                            "nwm_feature_id": 6444276,\r
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",\r
                            "huc12": "031300060207",\r
                            "units": "CFS"\r
                        },\r
                        "values": {\r
                            "year_1_5": 14110.33,\r
                            "year_2_0": 18327.25,\r
                            "year_3_0": 28842.9,\r
                            "year_4_0": 30716.7,\r
                            "year_5_0": 32276.83,\r
                            "year_10_0": 43859.19\r
                        }\r
                    }\r
                ]\r
            }\r
            """;

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
        Assert.assertEquals( 3, groupedLocations.size() );

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
            Map<WrdsLocation, Set<Threshold>> results =
                    activeCheckedThresholds.getThresholds( WRDSThresholdType.FLOW,
                                                           Operator.GREATER,
                                                           ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                           true,
                                                           this.unitMapper );

            Set<Threshold> thresholds = results.values().iterator().next();
            Map<String, Double> expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "low", 0.0 );

            for ( Threshold threshold : thresholds )
            {
                Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     threshold.getDataType() );
                Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( threshold.getName() ) );

                Assert.assertEquals( expectedThresholdValues.get( threshold.getName() ),
                                     threshold.getLeftThresholdValue().getValue(),
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
            results = activeCheckedThresholds.getThresholds( WRDSThresholdType.FLOW,
                                                             Operator.GREATER,
                                                             ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                             true,
                                                             this.unitMapper );

            thresholds = results.values().iterator().next();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "low", 0.0 );

            for ( Threshold threshold : thresholds )
            {
                Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     threshold.getDataType() );
                Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( threshold.getName() ) );

                Assert.assertEquals( expectedThresholdValues.get( threshold.getName() ),
                                     threshold.getLeftThresholdValue().getValue(),
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

            for ( Threshold threshold : thresholds )
            {
                Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     threshold.getDataType() );
                Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( threshold.getName() ) );

                Assert.assertEquals( expectedThresholdValues.get( threshold.getName() ),
                                     threshold.getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            iterator.next(); //Skip the 4th.
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
            expectedThresholdValues.put( "bankfull", 15.0 );
            expectedThresholdValues.put( "action", 13.0 );
            expectedThresholdValues.put( "flood", 17.0 );
            expectedThresholdValues.put( "minor", 17.0 );
            expectedThresholdValues.put( "moderate", 23.5 );
            expectedThresholdValues.put( "major", 26.0 );
            expectedThresholdValues.put( "record", 28.6 );

            for ( Threshold threshold : thresholds )
            {
                Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     threshold.getDataType() );
                Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( threshold.getName() ) );

                Assert.assertEquals( expectedThresholdValues.get( threshold.getName() ),
                                     threshold.getLeftThresholdValue().getValue(),
                                     EPSILON );
            }

            //Check the values with calculated flow included.
            results = activeCheckedThresholds.getThresholds( WRDSThresholdType.FLOW,
                                                             Operator.GREATER,
                                                             ThresholdConstants.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                             true,
                                                             this.unitMapper );

            thresholds = results.values().iterator().next();
            expectedThresholdValues = new HashMap<>();
            expectedThresholdValues.put( "flood", 36900.0 );
            expectedThresholdValues.put( "record", 209000.0 );

            expectedThresholdValues.put( "NRLDB bankfull", 38633.0 );
            expectedThresholdValues.put( "NRLDB action", 31313.0 );
            expectedThresholdValues.put( "NRLDB flood", 48628.0 );
            expectedThresholdValues.put( "NRLDB minor", 48628.0 );
            expectedThresholdValues.put( "NRLDB moderate", 144077.0 );
            expectedThresholdValues.put( "NRLDB major", 216266.0 );

            for ( Threshold threshold : thresholds )
            {
                Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     threshold.getDataType() );
                Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( threshold.getName() ) );

                Assert.assertEquals( expectedThresholdValues.get( threshold.getName() ),
                                     threshold.getLeftThresholdValue().getValue(),
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
            expectedThresholdValues.put( "flood", 36900.0 );
            expectedThresholdValues.put( "record", 209000.0 );

            for ( Threshold threshold : thresholds )
            {
                Assert.assertEquals( Threshold.ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                     threshold.getDataType() );
                Assert.assertEquals( Threshold.ThresholdOperator.GREATER, threshold.getOperator() );
                Assert.assertTrue( expectedThresholdValues.containsKey( threshold.getName() ) );

                Assert.assertEquals( expectedThresholdValues.get( threshold.getName() ),
                                     threshold.getLeftThresholdValue().getValue(),
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
                                                                                                        null,
                                                                                                        //null ratings provider.
                                                                                                        "stage",
                                                                                                        LeftOrRightOrBaseline.LEFT ),
                                                                           ThresholdOperator.GREATER_THAN );

            Map<WrdsLocation, Set<Threshold>> readThresholds =
                    GeneralWRDSReader.readThresholds( systemSettings,
                                                      normalThresholdConfig,
                                                      this.unitMapper,
                                                      FeatureDimension.NWS_LID,
                                                      DESIRED_FEATURES.stream()
                                                                      .map( WrdsLocation::nwsLid )
                                                                      .collect(
                                                                              Collectors.toSet() ) );

            Assert.assertTrue( readThresholds.containsKey( PTSA1 ) );
            Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
            Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
            Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
            Assert.assertTrue( readThresholds.containsKey( CEDG1 ) );

            //The two low thresholds available are identical in both label and value, so only one is included.
            Set<Threshold> ptsa1Thresholds = readThresholds.get( PTSA1 );
            Assert.assertEquals( 1, ptsa1Thresholds.size() );

            Set<Threshold> blof1Thresholds = readThresholds.get( BLOF1 );
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

            for ( Threshold threshold : blof1Thresholds )
            {
                String thresholdName = threshold.getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull" ->
                    {
                        hasBankfull = true;
                        Assert.assertEquals( 15.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "action" ->
                    {
                        hasAction = true;
                        Assert.assertEquals( 13.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "flood", "minor" ->
                    {
                        hasMinor = true;
                        Assert.assertEquals( 17.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "moderate" ->
                    {
                        hasModerate = true;
                        Assert.assertEquals( 23.5,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "major" ->
                    {
                        hasMajor = true;
                        Assert.assertEquals( 26.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "record" ->
                    {
                        hasRecord = true;
                        Assert.assertEquals( 28.6,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
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
                                                 null,
                                                 //null ratings provider.
                                                 null,
                                                 LeftOrRightOrBaseline.LEFT ),
                    ThresholdOperator.GREATER_THAN );

            Map<WrdsLocation, Set<Threshold>> readThresholds =
                    GeneralWRDSReader.readThresholds( systemSettings,
                                                      normalRecurrenceConfig,
                                                      this.unitMapper,
                                                      FeatureDimension.NWS_LID,
                                                      DESIRED_FEATURES.stream()
                                                                      .map( WrdsLocation::nwsLid )
                                                                      .collect(
                                                                              Collectors.toSet() ) );

            Assert.assertTrue( readThresholds.containsKey( PTSA1 ) );
            Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
            Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
            Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
            Assert.assertTrue( readThresholds.containsKey( CEDG1 ) );


            Set<Threshold> blof1Thresholds = readThresholds.get( BLOF1 );
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

            for ( Threshold threshold : blof1Thresholds )
            {
                String thresholdName = threshold.getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "year_1_5" ->
                    {
                        has1_5 = true;
                        Assert.assertEquals( 58864.26,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "year_2_0" ->
                    {
                        has2_0 = true;
                        Assert.assertEquals( 87362.48,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "year_3_0" ->
                    {
                        has3_0 = true;
                        Assert.assertEquals( 109539.05,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "year_4_0" ->
                    {
                        has4_0 = true;
                        Assert.assertEquals( 128454.64,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "year_5_0" ->
                    {
                        has5_0 = true;
                        Assert.assertEquals( 176406.6,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "year_10_0" ->
                    {
                        has10_0 = true;
                        Assert.assertEquals( 216831.58000000002,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
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

            Map<WrdsLocation, Set<Threshold>> readThresholds =
                    GeneralWRDSReader.readThresholds( systemSettings,
                                                      oldNormalThresholdConfig,
                                                      this.unitMapper,
                                                      FeatureDimension.NWS_LID,
                                                      DESIRED_FEATURES.stream()
                                                                      .map( WrdsLocation::nwsLid )
                                                                      .collect(
                                                                              Collectors.toSet() ) );

            Assert.assertTrue( readThresholds.containsKey( MNTG1 ) );
            Assert.assertTrue( readThresholds.containsKey( BLOF1 ) );
            Assert.assertTrue( readThresholds.containsKey( SMAF1 ) );
            Assert.assertTrue( readThresholds.containsKey( OKFG1 ) );
            Assert.assertTrue( readThresholds.containsKey( TLPT2 ) );

            Set<Threshold> mntg1Thresholds = readThresholds.get( MNTG1 );

            Assert.assertEquals( 6, mntg1Thresholds.size() );

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

            for ( Threshold threshold : mntg1Thresholds )
            {
                String thresholdName = threshold.getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull" ->
                    {
                        hasBankfull = true;
                        Assert.assertEquals( 11.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "action" ->
                    {
                        hasAction = true;
                        Assert.assertEquals( 11.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "minor" ->
                    {
                        hasMinor = true;
                        Assert.assertEquals( 20.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "moderate" ->
                    {
                        hasModerate = true;
                        Assert.assertEquals( 28.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "major" ->
                    {
                        hasMajor = true;
                        Assert.assertEquals( 31.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "record" ->
                    {
                        hasRecord = true;
                        Assert.assertEquals( 34.11,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                }
            }

            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasModerate );
            Assert.assertTrue( hasMajor );
            Assert.assertTrue( hasRecord );

            Set<Threshold> blof1Thresholds = readThresholds.get( BLOF1 );

            Assert.assertEquals( 6, blof1Thresholds.size() );

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

            for ( Threshold threshold : blof1Thresholds )
            {
                String thresholdName = threshold.getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull" ->
                    {
                        hasBankfull = true;
                        Assert.assertEquals( 15.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "action" ->
                    {
                        hasAction = true;
                        Assert.assertEquals( 13.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "minor" ->
                    {
                        hasMinor = true;
                        Assert.assertEquals( 17.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "moderate" ->
                    {
                        hasModerate = true;
                        Assert.assertEquals( 23.5,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "major" ->
                    {
                        hasMajor = true;
                        Assert.assertEquals( 26.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "record" ->
                    {
                        hasRecord = true;
                        Assert.assertEquals( 28.6,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                }
            }

            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasModerate );
            Assert.assertTrue( hasMajor );
            Assert.assertTrue( hasRecord );

            Set<Threshold> smaf1Thresholds = readThresholds.get( SMAF1 );

            Assert.assertEquals( 5, smaf1Thresholds.size() );

            hasBankfull = false;
            hasAction = false;
            hasMinor = false;
            hasModerate = false;
            hasMajor = false;
            hasRecord = false;

            properThresholds = List.of( "action",
                                        "minor",
                                        "moderate",
                                        "major",
                                        "record" );

            for ( Threshold threshold : smaf1Thresholds )
            {
                String thresholdName = threshold.getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "action" ->
                    {
                        hasAction = true;
                        Assert.assertEquals( 8.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "minor" ->
                    {
                        hasMinor = true;
                        Assert.assertEquals( 9.5,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "moderate" ->
                    {
                        hasModerate = true;
                        Assert.assertEquals( 11.5,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "major" ->
                    {
                        hasMajor = true;
                        Assert.assertEquals( 13.5,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "record" ->
                    {
                        hasRecord = true;
                        Assert.assertEquals( 15.36,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                }
            }

            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasModerate );
            Assert.assertTrue( hasMajor );
            Assert.assertTrue( hasRecord );

            Set<Threshold> okfg1Thresholds = readThresholds.get( OKFG1 );

            Assert.assertEquals( 4, okfg1Thresholds.size() );

            hasAction = false;
            hasMinor = false;
            hasRecord = false;

            properThresholds = List.of(
                    "bankfull",
                    "action",
                    "minor",
                    "record" );

            for ( Threshold threshold : okfg1Thresholds )
            {
                String thresholdName = threshold.getName()
                                                .toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull" ->
                    {
                        hasBankfull = true;
                        Assert.assertEquals( 0.0,
                                             threshold.getLeftThresholdValue()
                                                      .getValue(),
                                             EPSILON );
                    }
                    case "action" ->
                    {
                        hasAction = true;
                        Assert.assertEquals( 18.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "minor" ->
                    {
                        hasMinor = true;
                        Assert.assertEquals( 23.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "record" ->
                    {
                        hasRecord = true;
                        Assert.assertEquals( 40.1,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                }
            }

            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasAction );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasRecord );

            Set<Threshold> tlpt2Thresholds = readThresholds.get( TLPT2 );

            Assert.assertEquals( 3, tlpt2Thresholds.size() );

            hasBankfull = false;
            hasMinor = false;
            hasRecord = false;

            properThresholds = List.of(
                    "bankfull",
                    "minor",
                    "record" );

            for ( Threshold threshold : tlpt2Thresholds )
            {
                String thresholdName = threshold.getName().toLowerCase();

                Assert.assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull" ->
                    {
                        hasBankfull = true;
                        Assert.assertEquals( 15.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "minor" ->
                    {
                        hasMinor = true;
                        Assert.assertEquals( 15.0,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                    case "record" ->
                    {
                        hasRecord = true;
                        Assert.assertEquals( 16.02,
                                             threshold.getLeftThresholdValue().getValue(),
                                             EPSILON );
                    }
                }
            }

            Assert.assertTrue( hasBankfull );
            Assert.assertTrue( hasMinor );
            Assert.assertTrue( hasRecord );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }
}
