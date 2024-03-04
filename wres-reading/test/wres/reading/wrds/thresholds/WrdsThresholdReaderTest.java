package wres.reading.wrds.thresholds;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.DoubleValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.units.UnitMapper;
import wres.reading.wrds.geography.Location;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link WrdsThresholdReader}.
 *
 * @author James Brown
 * @author Hank Herr
 * @author Chris Tubbs
 */
class WrdsThresholdReaderTest
{
    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Test file name. */
    private static final String TEST_JSON = "test.json";

    /** Re-used string. */
    private static final String TEST = "test";

    /**
     * <p>The response used is created from this URL:
     * <a href="https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/">https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/</a>
     *
     * <p>Executed on 5/20/2021 at 10:15am.
     */
    private static final String ANOTHER_RESPONSE = """
            {
                "_metrics": {
                    "threshold_count": 10,
                    "total_request_time": 0.39240050315856934
                },
                "_warnings": [],
                "_documentation": {
                    "swagger URL": "redacted/docs/location/v3.0/swagger/"
                },
                "deployment": {
                    "api_url": "https://redacted/api/location/v3.0/nws_threshold/nws_lid/PTSA1,MNTG1,BLOF1,CEDG1,SMAF1/",
                    "stack": "prod",
                    "version": "v3.1.0"
                },
                "data_sources": {
                    "metadata_sources": [
                        "NWS data: NRLDB - Last updated: 2021-05-04 17:44:31 UTC",
                        "USGS data: USGS NWIS - Last updated: 2021-05-04 17:15:04 UTC"
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
                "value_set": [
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "PTSA1",
                            "usgs_site_code": "02372250",
                            "nwm_feature_id": "2323396",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "PTSA1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "PTSA1",
                            "usgs_site_code": "02372250",
                            "nwm_feature_id": "2323396",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02372250",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "MNTG1",
                            "usgs_site_code": "02349605",
                            "nwm_feature_id": "6444276",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": -0.16,
                            "bankfull": 11.0,
                            "action": 11.0,
                            "flood": 20.0,
                            "minor": 20.0,
                            "moderate": 28.0,
                            "major": 31.0,
                            "record": 34.11
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 11900.0,
                            "flood": 31500.0,
                            "minor": 31500.0,
                            "moderate": 77929.0,
                            "major": 105100.0,
                            "record": 136000.0
                        },
                        "calc_flow_values": {
                            "low": 557.8,
                            "bankfull": 9379.0,
                            "action": 9379.0,
                            "flood": 35331.0,
                            "minor": 35331.0,
                            "moderate": 102042.0,
                            "major": 142870.0,
                            "record": null,
                            "rating_curve": {
                                "location_id": "MNTG1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "MNTG1",
                            "usgs_site_code": "02349605",
                            "nwm_feature_id": "6444276",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": -0.16,
                            "bankfull": 11.0,
                            "action": 11.0,
                            "flood": 20.0,
                            "minor": 20.0,
                            "moderate": 28.0,
                            "major": 31.0,
                            "record": 34.11
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 11900.0,
                            "flood": 31500.0,
                            "minor": 31500.0,
                            "moderate": 77929.0,
                            "major": 105100.0,
                            "record": 136000.0
                        },
                        "calc_flow_values": {
                            "low": 554.06,
                            "bankfull": 9390.0,
                            "action": 9390.0,
                            "flood": 35329.0,
                            "minor": 35329.0,
                            "moderate": 102040.6,
                            "major": 142867.9,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02349605",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "BLOF1",
                            "usgs_site_code": "02358700",
                            "nwm_feature_id": "2297254",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": null,
                            "bankfull": 15.0,
                            "action": 13.0,
                            "flood": 17.0,
                            "minor": 17.0,
                            "moderate": 23.5,
                            "major": 26.0,
                            "record": 28.6
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": 36900.0,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 209000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": 38633.0,
                            "action": 31313.0,
                            "flood": 48628.0,
                            "minor": 48628.0,
                            "moderate": 144077.0,
                            "major": 216266.0,
                            "record": null,
                            "rating_curve": {
                                "location_id": "BLOF1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "BLOF1",
                            "usgs_site_code": "02358700",
                            "nwm_feature_id": "2297254",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": null,
                            "bankfull": 15.0,
                            "action": 13.0,
                            "flood": 17.0,
                            "minor": 17.0,
                            "moderate": 23.5,
                            "major": 26.0,
                            "record": 28.6
                        },
                        "flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": 36900.0,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 209000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": 36928.1,
                            "action": 30031.5,
                            "flood": 46234.6,
                            "minor": 46234.6,
                            "moderate": 133995.6,
                            "major": 205562.6,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02358700",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "CEDG1",
                            "usgs_site_code": "02343940",
                            "nwm_feature_id": "2310009",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 14.29
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "CEDG1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "CEDG1",
                            "usgs_site_code": "02343940",
                            "nwm_feature_id": "2310009",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 14.29
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "02343940",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "SMAF1",
                            "usgs_site_code": "02359170",
                            "nwm_feature_id": "2298964",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": 8.0,
                            "flood": 9.5,
                            "minor": 9.5,
                            "moderate": 11.5,
                            "major": 13.5,
                            "record": 15.36
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 179000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 45700.0,
                            "flood": 67700.0,
                            "minor": 67700.0,
                            "moderate": 107000.0,
                            "major": 159000.0,
                            "record": 221000.0,
                            "rating_curve": {
                                "location_id": "SMAF1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": "SMAF1",
                            "usgs_site_code": "02359170",
                            "nwm_feature_id": "2298964",
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "National Weather Service - National River Location Database"
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": 8.0,
                            "flood": 9.5,
                            "minor": 9.5,
                            "moderate": 11.5,
                            "major": 13.5,
                            "record": 15.36
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": 179000.0
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": 47355.4,
                            "flood": 69570.6,
                            "minor": 69570.6,
                            "moderate": 108505.8,
                            "major": 159463.2,
                            "record": 218963.05,
                            "rating_curve": {
                                "location_id": "02359170",
                                "id_type": "USGS Gage",
                                "source": "USGS Rating Depot",
                                "description": "The EXSA rating curves provided by USGS",
                                "interpolation_method": "logarithmic",
                                "interpolation_description": "logarithmic"
                            }
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWS Stream Thresholds",
                            "nws_lid": null,
                            "usgs_site_code": null,
                            "nwm_feature_id": null,
                            "threshold_type": "all (stage,flow)",
                            "stage_units": "FT",
                            "flow_units": "CFS",
                            "calc_flow_units": "CFS",
                            "threshold_source": "NWS-NRLDB",
                            "threshold_source_description": "Fake location with null feature names. See issue #117090."
                        },
                        "stage_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "flow_values": {
                            "low": 0.0,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null
                        },
                        "calc_flow_values": {
                            "low": null,
                            "bankfull": null,
                            "action": null,
                            "flood": null,
                            "minor": null,
                            "moderate": null,
                            "major": null,
                            "record": null,
                            "rating_curve": {
                                "location_id": "PTSA1",
                                "id_type": "NWS Station",
                                "source": "NRLDB",
                                "description": "National Weather Service - National River Location Database",
                                "interpolation_method": null,
                                "interpolation_description": null
                            }
                        }
                    }
                ]
            }
            """;
    /**
     * <p>The response used is created from this URL:
     * <a href="https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/">https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1//</a>
     *
     * <p>Executed on 5/22/2021 in the afternoon.
     */
    private static final String YET_ANOTHER_RESPONSE = """
            {
                "_metrics": {
                    "recurrence_flow_count": 5,
                    "total_request_time": 0.30907535552978516
                },
                "_warnings": [],
                "_documentation": {
                    "swagger URL": "redacted/docs/location/v3.0/swagger/"
                },
                "deployment": {
                    "api_url": "https://redacted/api/location/v3.0/nwm_recurrence_flow/nws_lid/PTSA1,MNTG1,BLOF1,SMAF1,CEDG1/",
                    "stack": "prod",
                    "version": "v3.1.0"
                },
                "data_sources": {
                    "metadata_sources": [
                        "NWS data: NRLDB - Last updated: 2021-05-04 17:44:31 UTC",
                        "USGS data: USGS NWIS - Last updated: 2021-05-04 17:15:04 UTC"
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
                "value_set": [
                    {
                        "metadata": {
                            "data_type": "NWM Recurrence Flows",
                            "nws_lid": "BLOF1",
                            "usgs_site_code": "02358700",
                            "nwm_feature_id": 2297254,
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",
                            "huc12": "031300110404",
                            "units": "CFS"
                        },
                        "values": {
                            "year_1_5": 58864.26,
                            "year_2_0": 87362.48,
                            "year_3_0": 109539.05,
                            "year_4_0": 128454.64,
                            "year_5_0": 176406.6,
                            "year_10_0": 216831.58000000002
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWM Recurrence Flows",
                            "nws_lid": "SMAF1",
                            "usgs_site_code": "02359170",
                            "nwm_feature_id": 2298964,
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",
                            "huc12": "031300110803",
                            "units": "CFS"
                        },
                        "values": {
                            "year_1_5": 70810.5,
                            "year_2_0": 91810.02,
                            "year_3_0": 111115.84,
                            "year_4_0": 132437.43,
                            "year_5_0": 188351.43,
                            "year_10_0": 231709.0
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWM Recurrence Flows",
                            "nws_lid": "CEDG1",
                            "usgs_site_code": "02343940",
                            "nwm_feature_id": 2310009,
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",
                            "huc12": "031300040707",
                            "units": "CFS"
                        },
                        "values": {
                            "year_1_5": 933.72,
                            "year_2_0": 977.16,
                            "year_3_0": 1414.0,
                            "year_4_0": 1459.2,
                            "year_5_0": 1578.3500000000001,
                            "year_10_0": 2532.27
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWM Recurrence Flows",
                            "nws_lid": "PTSA1",
                            "usgs_site_code": "02372250",
                            "nwm_feature_id": 2323396,
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",
                            "huc12": "031403020501",
                            "units": "CFS"
                        },
                        "values": {
                            "year_1_5": 3165.61,
                            "year_2_0": 4641.05,
                            "year_3_0": 6824.56,
                            "year_4_0": 7885.41,
                            "year_5_0": 9001.5,
                            "year_10_0": 11249.98
                        }
                    },
                    {
                        "metadata": {
                            "data_type": "NWM Recurrence Flows",
                            "nws_lid": "MNTG1",
                            "usgs_site_code": "02349605",
                            "nwm_feature_id": 6444276,
                            "nwm_location_crosswalk_dataset": "National Water Model v2.1 Corrected",
                            "huc12": "031300060207",
                            "units": "CFS"
                        },
                        "values": {
                            "year_1_5": 14110.33,
                            "year_2_0": 18327.25,
                            "year_3_0": 28842.9,
                            "year_4_0": 30716.7,
                            "year_5_0": 32276.83,
                            "year_10_0": 43859.19
                        }
                    }
                ]
            }
            """;

    /** Epsilon for matching doubles. */
    private static final double EPSILON = 0.00001;

    /** Stage unit string. */
    private static final String FT = "FT";

    // Various locations to test
    private static final Location PTSA1 = WrdsThresholdReaderTest.createFeature( "2323396", "02372250", "PTSA1" );
    private static final Location MNTG1 = WrdsThresholdReaderTest.createFeature( "6444276", "02349605", "MNTG1" );
    private static final Location BLOF1 = WrdsThresholdReaderTest.createFeature( "2297254", "02358700", "BLOF1" );
    private static final Location CEDG1 = WrdsThresholdReaderTest.createFeature( "2310009", "02343940", "CEDG1" );
    private static final Location SMAF1 = WrdsThresholdReaderTest.createFeature( "2298964", "02359170", "SMAF1" );
    private static final Location CHAF1 = WrdsThresholdReaderTest.createFeature( "2293124", "02358000", "CHAF1" );
    private static final Location OKFG1 = WrdsThresholdReaderTest.createFeature( "6447636", "02350512", "OKFG1" );
    private static final Location TLPT2 = WrdsThresholdReaderTest.createFeature( "13525368", "07311630", "TLPT2" );
    private static final Location NUTF1 = WrdsThresholdReaderTest.createFeature( null, null, "NUTF1" );
    private static final Location CDRA1 = WrdsThresholdReaderTest.createFeature( null, null, "CDRA1" );
    private static final Location MUCG1 = WrdsThresholdReaderTest.createFeature( null, null, "MUCG1" );
    private static final Location PRSG1 = WrdsThresholdReaderTest.createFeature( null, null, "PRSG1" );
    private static final Location LSNO2 = WrdsThresholdReaderTest.createFeature( null, null, "LSNO2" );
    private static final Location HDGA4 = WrdsThresholdReaderTest.createFeature( null, null, "HDGA4" );
    private static final Location FAKE3 = WrdsThresholdReaderTest.createFeature( null, null, "FAKE3" );
    private static final Location CNMP1 = WrdsThresholdReaderTest.createFeature( null, null, "CNMP1" );
    private static final Location WLLM2 = WrdsThresholdReaderTest.createFeature( null, null, "WLLM2" );
    private static final Location RCJD2 = WrdsThresholdReaderTest.createFeature( null, null, "RCJD2" );
    private static final Location MUSM5 = WrdsThresholdReaderTest.createFeature( null, null, "MUSM5" );
    private static final Location DUMM5 = WrdsThresholdReaderTest.createFeature( null, null, "DUMM5" );
    private static final Location DMTM5 = WrdsThresholdReaderTest.createFeature( null, null, "DMTM5" );
    private static final Location PONS2 = WrdsThresholdReaderTest.createFeature( null, null, "PONS2" );
    private static final Location MCKG1 = WrdsThresholdReaderTest.createFeature( null, null, "MCKG1" );
    private static final Location DSNG1 = WrdsThresholdReaderTest.createFeature( null, null, "DSNG1" );
    private static final Location BVAW2 = WrdsThresholdReaderTest.createFeature( null, null, "BVAW2" );
    private static final Location CNEO2 = WrdsThresholdReaderTest.createFeature( null, null, "CNEO2" );
    private static final Location CMKT2 = WrdsThresholdReaderTest.createFeature( null, null, "CMKT2" );
    private static final Location BDWN6 = WrdsThresholdReaderTest.createFeature( null, null, "BDWN6" );
    private static final Location CFBN6 = WrdsThresholdReaderTest.createFeature( null, null, "CFBN6" );
    private static final Location CCSA1 = WrdsThresholdReaderTest.createFeature( null, null, "CCSA1" );
    private static final Location LGNN8 = WrdsThresholdReaderTest.createFeature( null, null, "LGNN8" );
    private static final Location BCLN7 = WrdsThresholdReaderTest.createFeature( null, null, "BCLN7" );
    private static final Location KERV2 = WrdsThresholdReaderTest.createFeature( null, null, "KERV2" );
    private static final Location ARDS1 = WrdsThresholdReaderTest.createFeature( null, null, "ARDS1" );
    private static final Location WINW2 = WrdsThresholdReaderTest.createFeature( null, null, "WINW2" );
    private static final Location SRDN5 = WrdsThresholdReaderTest.createFeature( null, null, "SRDN5" );
    private static final Location MNTN1 = WrdsThresholdReaderTest.createFeature( null, null, "MNTN1" );
    private static final Location GNSW4 = WrdsThresholdReaderTest.createFeature( null, null, "GNSW4" );
    private static final Location JAIO1 = WrdsThresholdReaderTest.createFeature( null, null, "JAIO1" );
    private static final Location INCO1 = WrdsThresholdReaderTest.createFeature( null, null, "INCO1" );
    private static final Location PRMO1 = WrdsThresholdReaderTest.createFeature( null, null, "PRMO1" );
    private static final Location PARO1 = WrdsThresholdReaderTest.createFeature( null, null, "PARO1" );
    private static final Location BRCO1 = WrdsThresholdReaderTest.createFeature( null, null, "BRCO1" );
    private static final Location WRNO1 = WrdsThresholdReaderTest.createFeature( null, null, "WRNO1" );
    private static final Location BLEO1 = WrdsThresholdReaderTest.createFeature( null, null, "BLEO1" );

    private static final List<Location> DESIRED_FEATURES = List.of( PTSA1,
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

    /** A unit mapper for stage. */
    private UnitMapper stageMapper;

    /** A unit mapper for flow. */
    private UnitMapper flowMapper;

    /** A reader to test. */
    private WrdsThresholdReader reader;

    @BeforeEach
    void runBeforeEachTest()
    {
        this.reader = WrdsThresholdReader.of();
        this.stageMapper = UnitMapper.of( "FT" );
        this.flowMapper = UnitMapper.of( "CFS" );
        this.mockServer = ClientAndServer.startClientAndServer( 0 );
    }

    @AfterEach
    void stopServer()
    {
        this.mockServer.stop();
    }

    @Test
    void testReadThresholdsFromMockedService()
    {
        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + "/redacted/api/location/v3.0/nws_threshold/" );

        String expectedPath = "/redacted/api/location/v3.0/nws_threshold/nws_lid/BLOF1,CEDG1,MNTG1,PTSA1,SMAF1/";
        this.mockServer.when( HttpRequest.request()
                                         .withPath( expectedPath )
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response( ANOTHER_RESPONSE ) );

        ThresholdSource service
                = ThresholdSourceBuilder.builder()
                                        .uri( fakeUri )
                                        .featureNameFrom( DatasetOrientation.LEFT )
                                        .type( wres.config.yaml.components.ThresholdType.VALUE )
                                        .parameter( "stage" )
                                        .provider( "NWS-NRLDB" )
                                        .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                        .build();

        Set<wres.config.yaml.components.Threshold> actual =
                this.reader.read( service,
                                  this.stageMapper,
                                  Set.of( "PTSA1", "MNTG1", "BLOF1", "CEDG1", "SMAF1" ),
                                  FeatureAuthority.NWS_LID );

        Threshold firstOne = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 0.0 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "low" )
                                      .build();
        Threshold firstTwo = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 8.0 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "action" )
                                      .build();
        Threshold firstThree = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 9.5 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "flood" )
                                        .build();
        Threshold firstFour = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 9.5 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "minor" )
                                       .build();
        Threshold firstFive = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 11.5 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "moderate" )
                                       .build();
        Threshold firstSix = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 13.5 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "major" )
                                      .build();
        Threshold firstSeven = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 15.36 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "record" )
                                        .build();

        Set<Threshold> firstExpectedInner = Set.of( firstOne,
                                                    firstTwo,
                                                    firstThree,
                                                    firstFour,
                                                    firstFive,
                                                    firstSix,
                                                    firstSeven );

        Set<wres.config.yaml.components.Threshold> firstExpected =
                WrdsThresholdReaderTest.createThresholds( firstExpectedInner,
                                                          "SMAF1" );

        Set<wres.config.yaml.components.Threshold> firstActual = WrdsThresholdReaderTest.filter( actual, "SMAF1" );

        assertEquals( firstExpected, firstActual );

        Threshold secondOne = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 0.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "low" )
                                       .build();
        Threshold secondTwo = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 14.29 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "record" )
                                       .build();

        Set<Threshold> secondExpectedInner = Set.of( secondOne,
                                                     secondTwo );

        Set<wres.config.yaml.components.Threshold> secondExpected =
                WrdsThresholdReaderTest.createThresholds( secondExpectedInner,
                                                          "CEDG1" );

        Set<wres.config.yaml.components.Threshold> secondActual = WrdsThresholdReaderTest.filter( actual, "CEDG1" );

        assertEquals( secondExpected, secondActual );

        Threshold thirdOne = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 0.0 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "low" )
                                      .build();

        Set<Threshold> thirdExpectedInner = Set.of( thirdOne );

        Set<wres.config.yaml.components.Threshold> thirdExpected =
                WrdsThresholdReaderTest.createThresholds( thirdExpectedInner,
                                                          "PTSA1" );

        Set<wres.config.yaml.components.Threshold> thirdActual = WrdsThresholdReaderTest.filter( actual, "PTSA1" );

        assertEquals( thirdExpected, thirdActual );

        Threshold fourthOne = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 13.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "action" )
                                       .build();
        Threshold fourthTwo = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 15.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "bankfull" )
                                       .build();
        Threshold fourthThree = Threshold.newBuilder()
                                         .setOperator( Threshold.ThresholdOperator.GREATER )
                                         .setLeftThresholdValue( DoubleValue.of( 17.0 ) )
                                         .setThresholdValueUnits( FT )
                                         .setName( "flood" )
                                         .build();
        Threshold fourthFour = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 17.0 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "minor" )
                                        .build();
        Threshold fourthFive = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 23.5 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "moderate" )
                                        .build();
        Threshold fourthSix = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 26.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "major" )
                                       .build();
        Threshold fourthSeven = Threshold.newBuilder()
                                         .setOperator( Threshold.ThresholdOperator.GREATER )
                                         .setLeftThresholdValue( DoubleValue.of( 28.6 ) )
                                         .setThresholdValueUnits( FT )
                                         .setName( "record" )
                                         .build();

        Set<Threshold> fourthExpectedInner = Set.of( fourthOne,
                                                     fourthTwo,
                                                     fourthThree,
                                                     fourthFour,
                                                     fourthFive,
                                                     fourthSix,
                                                     fourthSeven );

        Set<wres.config.yaml.components.Threshold> fourthExpected =
                WrdsThresholdReaderTest.createThresholds( fourthExpectedInner,
                                                          "BLOF1" );

        Set<wres.config.yaml.components.Threshold> fourthActual = WrdsThresholdReaderTest.filter( actual, "BLOF1" );

        assertEquals( fourthExpected, fourthActual );

        Threshold fifthOne = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( -0.16 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "low" )
                                      .build();
        Threshold fifthTwo = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 11.0 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "action" )
                                      .build();
        Threshold fifthThree = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 11.0 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "bankfull" )
                                        .build();
        Threshold fifthFour = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 20.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "flood" )
                                       .build();
        Threshold fifthFive = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 20.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "minor" )
                                       .build();
        Threshold fifthSix = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 28.0 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "moderate" )
                                      .build();
        Threshold fifthSeven = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 31.0 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "major" )
                                        .build();
        Threshold fifthEighth = Threshold.newBuilder()
                                         .setOperator( Threshold.ThresholdOperator.GREATER )
                                         .setLeftThresholdValue( DoubleValue.of( 34.11 ) )
                                         .setThresholdValueUnits( FT )
                                         .setName( "record" )
                                         .build();
        Set<Threshold> fifthExpectedInner = Set.of( fifthOne,
                                                    fifthTwo,
                                                    fifthThree,
                                                    fifthFour,
                                                    fifthFive,
                                                    fifthSix,
                                                    fifthSeven,
                                                    fifthEighth );

        Set<wres.config.yaml.components.Threshold> fifthExpected =
                WrdsThresholdReaderTest.createThresholds( fifthExpectedInner,
                                                          "MNTG1" );

        Set<wres.config.yaml.components.Threshold> fifthActual = WrdsThresholdReaderTest.filter( actual, "MNTG1" );

        assertEquals( fifthExpected, fifthActual );
    }

    @Test
    void testReadThresholdsFromMockedServiceWithChunksAndDuplicates()
    {
        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + "/redacted/api/location/v3.0/nws_threshold/" );

        String expectedPathOne = "/redacted/api/location/v3.0/nws_threshold/nws_lid/BLOF1/";
        this.mockServer.when( HttpRequest.request()
                                         .withPath( expectedPathOne )
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response( ANOTHER_RESPONSE ) );

        String expectedPathTwo = "/redacted/api/location/v3.0/nws_threshold/nws_lid/CEDG1/";
        this.mockServer.when( HttpRequest.request()
                                         .withPath( expectedPathTwo )
                                         .withMethod( "GET" ) )
                       .respond( HttpResponse.response( ANOTHER_RESPONSE ) );

        ThresholdSource service
                = ThresholdSourceBuilder.builder()
                                        .uri( fakeUri )
                                        .featureNameFrom( DatasetOrientation.LEFT )
                                        .type( wres.config.yaml.components.ThresholdType.VALUE )
                                        .parameter( "stage" )
                                        .provider( "NWS-NRLDB" )
                                        .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                        .build();

        WrdsThresholdReader chunkedReader = WrdsThresholdReader.of( 1 );

        Set<wres.config.yaml.components.Threshold> actual =
                chunkedReader.read( service,
                                    this.stageMapper,
                                    Set.of( "BLOF1", "CEDG1" ),
                                    FeatureAuthority.NWS_LID );

        Threshold firstOne = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 0.0 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "low" )
                                      .build();
        Threshold firstTwo = Threshold.newBuilder()
                                      .setOperator( Threshold.ThresholdOperator.GREATER )
                                      .setLeftThresholdValue( DoubleValue.of( 14.29 ) )
                                      .setThresholdValueUnits( FT )
                                      .setName( "record" )
                                      .build();

        Set<Threshold> firstExpectedInner = Set.of( firstOne,
                                                    firstTwo );

        Set<wres.config.yaml.components.Threshold> firstExpected =
                WrdsThresholdReaderTest.createThresholds( firstExpectedInner,
                                                          "CEDG1" );

        Set<wres.config.yaml.components.Threshold> firstActual = WrdsThresholdReaderTest.filter( actual, "CEDG1" );

        assertEquals( firstExpected, firstActual );

        Threshold secondOne = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 13.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "action" )
                                       .build();
        Threshold secondTwo = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 15.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "bankfull" )
                                       .build();
        Threshold secondThree = Threshold.newBuilder()
                                         .setOperator( Threshold.ThresholdOperator.GREATER )
                                         .setLeftThresholdValue( DoubleValue.of( 17.0 ) )
                                         .setThresholdValueUnits( FT )
                                         .setName( "flood" )
                                         .build();
        Threshold secondFour = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 17.0 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "minor" )
                                        .build();
        Threshold secondFive = Threshold.newBuilder()
                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                        .setLeftThresholdValue( DoubleValue.of( 23.5 ) )
                                        .setThresholdValueUnits( FT )
                                        .setName( "moderate" )
                                        .build();
        Threshold secondSix = Threshold.newBuilder()
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setLeftThresholdValue( DoubleValue.of( 26.0 ) )
                                       .setThresholdValueUnits( FT )
                                       .setName( "major" )
                                       .build();
        Threshold secondSeven = Threshold.newBuilder()
                                         .setOperator( Threshold.ThresholdOperator.GREATER )
                                         .setLeftThresholdValue( DoubleValue.of( 28.6 ) )
                                         .setThresholdValueUnits( FT )
                                         .setName( "record" )
                                         .build();

        Set<Threshold> secondExpectedInner = Set.of( secondOne,
                                                     secondTwo,
                                                     secondThree,
                                                     secondFour,
                                                     secondFive,
                                                     secondSix,
                                                     secondSeven );

        Set<wres.config.yaml.components.Threshold> secondExpected =
                WrdsThresholdReaderTest.createThresholds( secondExpectedInner,
                                                          "BLOF1" );

        Set<wres.config.yaml.components.Threshold> secondActual = WrdsThresholdReaderTest.filter( actual, "BLOF1" );

        assertEquals( secondExpected, secondActual );
    }

    @Test
    void testReadThresholdsFromFileSystem() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new WRDS JSON service response to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( ANOTHER_RESPONSE );
            }

            ThresholdSource service
                    = ThresholdSourceBuilder.builder()
                                            .uri( jsonPath.toUri() )
                                            .featureNameFrom( DatasetOrientation.LEFT )
                                            .type( wres.config.yaml.components.ThresholdType.VALUE )
                                            .parameter( "stage" )
                                            .provider( "NWS-NRLDB" )
                                            .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                            .build();

            Set<wres.config.yaml.components.Threshold> readThresholds =
                    this.reader.read( service,
                                      this.stageMapper,
                                      DESIRED_FEATURES.stream()
                                                      .map( Location::nwsLid )
                                                      .collect( Collectors.toSet() ),
                                      FeatureAuthority.NWS_LID );

            Set<wres.config.yaml.components.Threshold> ptsa1Thresholds = WrdsThresholdReaderTest.filter( readThresholds,
                                                                                                         "PTSA1" );
            assertFalse( ptsa1Thresholds.isEmpty() );
            Set<wres.config.yaml.components.Threshold> blof1Thresholds = WrdsThresholdReaderTest.filter( readThresholds,
                                                                                                         "BLOF1" );
            assertFalse( blof1Thresholds.isEmpty() );
            assertFalse( WrdsThresholdReaderTest.filter( readThresholds,
                                                         "MNTG1" )
                                                .isEmpty() );
            assertFalse( WrdsThresholdReaderTest.filter( readThresholds,
                                                         "SMAF1" )
                                                .isEmpty() );
            assertFalse( WrdsThresholdReaderTest.filter( readThresholds,
                                                         "CEDG1" )
                                                .isEmpty() );

            //The two low thresholds available are identical in both label and value, so only one is included.
            assertEquals( 1, ptsa1Thresholds.size() );
            assertEquals( 7, blof1Thresholds.size() );

            boolean hasLow = false;
            boolean hasBankfull = false;
            boolean hasAction = false;
            boolean hasMinor = false;
            boolean hasModerate = false;
            boolean hasMajor = false;
            boolean hasRecord = false;

            List<String> properThresholds = List.of( "bankfull",
                                                     "action",
                                                     "flood",
                                                     "minor",
                                                     "moderate",
                                                     "major",
                                                     "record" );

            for ( wres.config.yaml.components.Threshold wrappedThreshold : blof1Thresholds )
            {
                Threshold threshold = wrappedThreshold.threshold();
                String thresholdName = threshold.getName().toLowerCase();

                assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "bankfull" ->
                    {
                        hasBankfull = true;
                        assertEquals( 15.0,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "action" ->
                    {
                        hasAction = true;
                        assertEquals( 13.0,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "flood", "minor" ->
                    {
                        hasMinor = true;
                        assertEquals( 17.0,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "moderate" ->
                    {
                        hasModerate = true;
                        assertEquals( 23.5,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "major" ->
                    {
                        hasMajor = true;
                        assertEquals( 26.0,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "record" ->
                    {
                        hasRecord = true;
                        assertEquals( 28.6,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                }
            }

            assertFalse( hasLow );
            assertTrue( hasBankfull );
            assertTrue( hasAction );
            assertTrue( hasMinor );
            assertTrue( hasModerate );
            assertTrue( hasMajor );
            assertTrue( hasRecord );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }

    @Test
    void testReadRecurrenceFlowsFromFileSystem() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new WRDS JSON service response to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( YET_ANOTHER_RESPONSE );
            }

            ThresholdSource service
                    = ThresholdSourceBuilder.builder()
                                            .uri( jsonPath.toUri() )
                                            .featureNameFrom( DatasetOrientation.LEFT )
                                            .type( wres.config.yaml.components.ThresholdType.VALUE )
                                            .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                            .build();

            Set<wres.config.yaml.components.Threshold> readThresholds =
                    this.reader.read( service,
                                      this.flowMapper,
                                      DESIRED_FEATURES.stream()
                                                      .map( Location::nwsLid )
                                                      .collect( Collectors.toSet() ),
                                      FeatureAuthority.NWS_LID );

            Set<wres.config.yaml.components.Threshold> blof1Thresholds = WrdsThresholdReaderTest.filter( readThresholds,
                                                                                                         "BLOF1" );
            assertFalse( blof1Thresholds.isEmpty() );
            assertFalse( WrdsThresholdReaderTest.filter( readThresholds,
                                                         "PTSA1" )
                                                .isEmpty() );
            assertFalse( WrdsThresholdReaderTest.filter( readThresholds,
                                                         "MNTG1" )
                                                .isEmpty() );
            assertFalse( WrdsThresholdReaderTest.filter( readThresholds,
                                                         "SMAF1" )
                                                .isEmpty() );
            assertFalse( WrdsThresholdReaderTest.filter( readThresholds,
                                                         "CEDG1" )
                                                .isEmpty() );

            assertEquals( 6, blof1Thresholds.size() );

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

            for ( wres.config.yaml.components.Threshold wrappedThreshold : blof1Thresholds )
            {
                Threshold threshold = wrappedThreshold.threshold();
                String thresholdName = threshold.getName()
                                                .toLowerCase();

                assertTrue( properThresholds.contains( thresholdName ) );

                switch ( thresholdName )
                {
                    case "year_1_5" ->
                    {
                        has1_5 = true;
                        assertEquals( 58864.26,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "year_2_0" ->
                    {
                        has2_0 = true;
                        assertEquals( 87362.48,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "year_3_0" ->
                    {
                        has3_0 = true;
                        assertEquals( 109539.05,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "year_4_0" ->
                    {
                        has4_0 = true;
                        assertEquals( 128454.64,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "year_5_0" ->
                    {
                        has5_0 = true;
                        assertEquals( 176406.6,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                    case "year_10_0" ->
                    {
                        has10_0 = true;
                        assertEquals( 216831.58000000002,
                                      threshold.getLeftThresholdValue()
                                               .getValue(),
                                      EPSILON );
                    }
                }
            }

            assertTrue( has1_5 );
            assertTrue( has2_0 );
            assertTrue( has3_0 );
            assertTrue( has4_0 );
            assertTrue( has5_0 );
            assertTrue( has10_0 );

            // Clean up
            if ( Files.exists( jsonPath ) )
            {
                Files.delete( jsonPath );
            }
        }
    }

    @Test
    void testExtractThresholdsWithUnexpectedRatingProviderThrowsExpectedException() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new WRDS JSON service response to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( YET_ANOTHER_RESPONSE );
            }

            ThresholdSource service
                    = ThresholdSourceBuilder.builder()
                                            .uri( jsonPath.toUri() )
                                            .featureNameFrom( DatasetOrientation.LEFT )
                                            .type( wres.config.yaml.components.ThresholdType.VALUE )
                                            .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                            // No such rating provider
                                            .ratingProvider( "foofoo" )
                                            .build();

            NoThresholdsFoundException actual =
                    assertThrows( NoThresholdsFoundException.class, // NOSONAR
                                  () -> this.reader.read( service,
                                                          this.flowMapper,
                                                          DESIRED_FEATURES.stream()
                                                                          .map( Location::nwsLid )
                                                                          .collect(
                                                                                  Collectors.toSet() ),
                                                          FeatureAuthority.NWS_LID ) );

            String expectedMessagePart = "ratings provider 'foofoo', discovered no thresholds that match the ratings";

            assertTrue( actual.getMessage()
                              .contains( expectedMessagePart ) );
        }
    }

    @Test
    void testExtractThresholdsWithUnexpectedThresholdProviderThrowsExpectedException() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            // Write a new WRDS JSON service response to an in-memory file system
            Path directory = fileSystem.getPath( TEST );
            Files.createDirectory( directory );
            Path pathToStore = fileSystem.getPath( TEST, TEST_JSON );
            Path jsonPath = Files.createFile( pathToStore );

            try ( BufferedWriter writer = Files.newBufferedWriter( jsonPath ) )
            {
                writer.append( YET_ANOTHER_RESPONSE );
            }

            ThresholdSource service
                    = ThresholdSourceBuilder.builder()
                                            .uri( jsonPath.toUri() )
                                            .featureNameFrom( DatasetOrientation.LEFT )
                                            .type( wres.config.yaml.components.ThresholdType.VALUE )
                                            .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                            // No such provider
                                            .provider( "barbar" )
                                            .build();

            NoThresholdsFoundException actual =
                    assertThrows( NoThresholdsFoundException.class, // NOSONAR
                                  () -> this.reader.read( service,
                                                          this.flowMapper,
                                                          DESIRED_FEATURES.stream()
                                                                          .map( Location::nwsLid )
                                                                          .collect(
                                                                                  Collectors.toSet() ),
                                                          FeatureAuthority.NWS_LID ) );

            String expectedMessagePart = "provider 'barbar', discovered no thresholds that match the provider";

            assertTrue( actual.getMessage()
                              .contains( expectedMessagePart ) );
        }
    }

    /**
     * Creates a location from the inputs.
     *
     * @param featureId the feature ID
     * @param usgsSiteCode the USGS Site Code
     * @param lid the NWS LID
     * @return the location
     */
    private static Location createFeature( final String featureId, final String usgsSiteCode, final String lid )
    {
        return new Location( featureId, usgsSiteCode, lid );
    }

    /**
     * Creates a featureful thresholds from the inputs.
     * @param thresholds the thresholds
     * @param featureName the feature name
     * @return the featureful threshold
     */

    private static Set<wres.config.yaml.components.Threshold> createThresholds( Set<Threshold> thresholds,
                                                                                String featureName )
    {
        return thresholds.stream()
                         .map( n -> WrdsThresholdReaderTest.createThreshold( n, featureName ) )
                         .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Creates a featureful threshold from the inputs.
     * @param threshold the threshold
     * @param featureName the feature name
     * @return the featureful threshold
     */

    private static wres.config.yaml.components.Threshold createThreshold( Threshold threshold,
                                                                          String featureName )
    {
        Geometry geometry = Geometry.newBuilder()
                                    .setName( featureName )
                                    .build();

        return ThresholdBuilder.builder()
                               .threshold( threshold )
                               .feature( geometry )
                               .type( ThresholdType.VALUE )
                               .featureNameFrom( DatasetOrientation.LEFT )
                               .build();
    }

    /**
     * Filters the input thresholds for the named feature.
     * @param thresholds the thresholds
     * @param featureName the named feature
     * @return the filtered thresholds
     */

    private static Set<wres.config.yaml.components.Threshold> filter( Set<wres.config.yaml.components.Threshold> thresholds,
                                                                      String featureName )
    {
        return thresholds.stream()
                         .filter( n -> n.feature()
                                        .getName()
                                        .equals( featureName ) )
                         .collect( Collectors.toSet() );
    }
}
