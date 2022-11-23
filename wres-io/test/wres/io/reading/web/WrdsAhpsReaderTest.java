package wres.io.reading.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;
import org.mockserver.model.Parameters;
import org.mockserver.verify.VerificationTimes;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.NamedFeature;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.system.SystemSettings;

/**
 * Tests the {@link WrdsAhpsReader}.
 * @author James Brown
 */

class WrdsAhpsReaderTest
{
    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Feature considered. */
    private static final String FEATURE_NAME = "FROV2";

    /** Path used by GET for forecasts. */
    private static final String FORECAST_PATH_V2 =
            "/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/" + FEATURE_NAME + "/";

    /** Forecast response from GET. */
    private static final String FORECAST_RESPONSE_V2 = "{\n"
                                                       + "    \"_documentation\": {\n"
                                                       + "        \"swagger URL\": \"http://***REMOVED***.***REMOVED***.***REMOVED***/docs/rfc_forecast/v2.0/swagger/\"\n"
                                                       + "    },\n"
                                                       + "    \"deployment\": {\n"
                                                       + "        \"api_url\": \"https://***REMOVED***.***REMOVED***.***REMOVED***/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/FROV2/\",\n"
                                                       + "        \"stack\": \"prod\",\n"
                                                       + "        \"version\": \"v2.4.0\",\n"
                                                       + "        \"api_caller\": \"None\"\n"
                                                       + "    },\n"
                                                       + "    \"_metrics\": {\n"
                                                       + "        \"forecast_count\": 1,\n"
                                                       + "        \"location_count\": 1,\n"
                                                       + "        \"total_request_time\": 1.1423799991607666\n"
                                                       + "    },\n"
                                                       + "    \"header\": {\n"
                                                       + "        \"request\": {\n"
                                                       + "            \"params\": {\n"
                                                       + "                \"asProvided\": {},\n"
                                                       + "                \"asUsed\": {\n"
                                                       + "                    \"issuedTime\": \"latest\",\n"
                                                       + "                    \"validTime\": \"all\",\n"
                                                       + "                    \"excludePast\": \"False\",\n"
                                                       + "                    \"minForecastStatus\": \"no_flooding\",\n"
                                                       + "                    \"use_rating_curve\": null,\n"
                                                       + "                    \"includeDuplicates\": \"False\",\n"
                                                       + "                    \"returnNewestForecast\": \"False\",\n"
                                                       + "                    \"completeForecastStatus\": \"False\"\n"
                                                       + "                }\n"
                                                       + "            }\n"
                                                       + "        },\n"
                                                       + "        \"missing_values\": [\n"
                                                       + "            -999,\n"
                                                       + "            -9999\n"
                                                       + "        ]\n"
                                                       + "    },\n"
                                                       + "    \"forecasts\": [\n"
                                                       + "        {\n"
                                                       + "            \"location\": {\n"
                                                       + "                \"names\": {\n"
                                                       + "                    \"nwsLid\": \"FROV2\",\n"
                                                       + "                    \"usgsSiteCode\": \"01631000\",\n"
                                                       + "                    \"nwm_feature_id\": 5907079,\n"
                                                       + "                    \"nwsName\": \"Front Royal\",\n"
                                                       + "                    \"usgsName\": \"S F SHENANDOAH RIVER AT FRONT ROYAL, VA\"\n"
                                                       + "                },\n"
                                                       + "                \"coordinates\": {\n"
                                                       + "                    \"latitude\": 38.91400059,\n"
                                                       + "                    \"longitude\": -78.21083388,\n"
                                                       + "                    \"crs\": \"NAD83\",\n"
                                                       + "                    \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                       + "                },\n"
                                                       + "                \"nws_coordinates\": {\n"
                                                       + "                    \"latitude\": 38.913888888889,\n"
                                                       + "                    \"longitude\": -78.211111111111,\n"
                                                       + "                    \"crs\": \"NAD27\",\n"
                                                       + "                    \"link\": \"https://maps.google.com/maps?q=38.913888888889,-78.211111111111\"\n"
                                                       + "                },\n"
                                                       + "                \"usgs_coordinates\": {\n"
                                                       + "                    \"latitude\": 38.91400059,\n"
                                                       + "                    \"longitude\": -78.21083388,\n"
                                                       + "                    \"crs\": \"NAD83\",\n"
                                                       + "                    \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                       + "                }\n"
                                                       + "            },\n"
                                                       + "            \"producer\": \"MARFC\",\n"
                                                       + "            \"issuer\": \"LWX\",\n"
                                                       + "            \"distributor\": \"SBN\",\n"
                                                       + "            \"type\": \"deterministic\",\n"
                                                       + "            \"issuedTime\": \"2021-11-14T13:46:00Z\",\n"
                                                       + "            \"generationTime\": \"2021-11-14T13:54:18Z\",\n"
                                                       + "            \"parameterCodes\": {\n"
                                                       + "                \"physicalElement\": \"QR\",\n"
                                                       + "                \"duration\": \"I\",\n"
                                                       + "                \"typeSource\": \"FF\",\n"
                                                       + "                \"extremum\": \"Z\",\n"
                                                       + "                \"probability\": \"Z\"\n"
                                                       + "            },\n"
                                                       + "            \"thresholds\": {\n"
                                                       + "                \"action\": null,\n"
                                                       + "                \"minor\": 24340.0,\n"
                                                       + "                \"moderate\": 31490.0,\n"
                                                       + "                \"major\": 47200.0,\n"
                                                       + "                \"record\": 130000.0\n"
                                                       + "            },\n"
                                                       + "            \"units\": {\n"
                                                       + "                \"streamflow\": \"KCFS\"\n"
                                                       + "            },\n"
                                                       + "            \"members\": [\n"
                                                       + "                {\n"
                                                       + "                    \"identifier\": \"1\",\n"
                                                       + "                    \"forecast_status\": \"no_flooding\",\n"
                                                       + "                    \"dataPointsList\": [\n"
                                                       + "                        [\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-14T18:00:00Z\",\n"
                                                       + "                                \"value\": 2.12,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-15T00:00:00Z\",\n"
                                                       + "                                \"value\": 2.12,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-15T06:00:00Z\",\n"
                                                       + "                                \"value\": 2.01,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-15T12:00:00Z\",\n"
                                                       + "                                \"value\": 1.89,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-15T18:00:00Z\",\n"
                                                       + "                                \"value\": 1.78,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-16T00:00:00Z\",\n"
                                                       + "                                \"value\": 1.67,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-16T06:00:00Z\",\n"
                                                       + "                                \"value\": 1.67,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-16T12:00:00Z\",\n"
                                                       + "                                \"value\": 1.52,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-16T18:00:00Z\",\n"
                                                       + "                                \"value\": 1.52,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-17T00:00:00Z\",\n"
                                                       + "                                \"value\":  -9999.00,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-17T06:00:00Z\",\n"
                                                       + "                                \"value\": -999,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            },\n"
                                                       + "                            {\n"
                                                       + "                                \"time\": \"2021-11-17T12:00:00Z\",\n"
                                                       + "                                \"value\": 1.38,\n"
                                                       + "                                \"status\": \"no_flooding\"\n"
                                                       + "                            }\n"
                                                       + "                        ]\n"
                                                       + "                    ]\n"
                                                       + "                }\n"
                                                       + "            ]\n"
                                                       + "        }\n"
                                                       + "    ]\n"
                                                       + "}\n";

    /** Path used by GET for observations. */
    private static final String OBSERVED_PATH_V1 =
            "/api/observed/v1.0/observed/streamflow/nws_lid/";

    /** Path parameters used by GET for observations. */
    private static final String OBSERVED_PATH_PARAMS_V1 =
            "?proj=WRES&validTime=%5B2022-03-01T00%3A00%3A00Z%2C2022-03-01T14%3A12%3A59Z%5D";

    /** Observed response from GET. */
    private static final String OBSERVED_RESPONSE_V1 = "{\n"
                                                       + "    \"_documentation\": {\n"
                                                       + "        \"swaggerURL\": \"http://***REMOVED***.***REMOVED***.***REMOVED***/docs/observed/v1.0/swagger/\" \n"
                                                       + "    },\n"
                                                       + "    \"_deployment\": {\n"
                                                       + "        \"apiUrl\":\"https://***REMOVED***.***REMOVED***.***REMOVED***/api/observed/v1.0/observed/streamflow/nws_lid/FROV2/?proj=WRES&validTime=%5B2022-03-01T00%3A00%3A00Z%2C2022-03-01T14%3A12%3A59Z%5D\",\n"
                                                       + "        \"stack\": \"dev\",\n"
                                                       + "        \"version\": \"v1.0.0\",\n"
                                                       + "        \"apiCaller\": \"WRES\" \n"
                                                       + "    },\n"
                                                       + "    \"_nonUrgentIssueReportingLink\": \"https://vlab.***REMOVED***/redmine/projects/wrds-user-support/issues/new?issue[category_id]=2835\",\n"
                                                       + "    \"_metrics\": {\n"
                                                       + "        \"observedDataCount\": 64,\n"
                                                       + "        \"locationCount\": 2,\n"
                                                       + "        \"totalRequestTime\": 0.6576216220855713\n"
                                                       + "    },\n"
                                                       + "    \"header\": {\n"
                                                       + "        \"request\": {\n"
                                                       + "            \"params\": {\n"
                                                       + "                \"asProvided\": {\n"
                                                       + "                    \"validTime\": \"[2022-03-01T00:00:00Z,2022-03-01T14:12:59Z]\" \n"
                                                       + "                },\n"
                                                       + "                \"asUsed\": {\n"
                                                       + "                    \"validTime\": \"[2022-03-01T00:00:00Z,2022-03-01T14:12:59Z]\",\n"
                                                       + "                    \"nonUSGSGages\": false,\n"
                                                       + "                    \"pe\": null,\n"
                                                       + "                    \"ts\": null\n"
                                                       + "                }\n"
                                                       + "            }\n"
                                                       + "        },\n"
                                                       + "        \"missingValues\": [\n"
                                                       + "            -999,\n"
                                                       + "            -9999\n"
                                                       + "        ]\n"
                                                       + "    },\n"
                                                       + "    \"timeseriesDataset\": [\n"
                                                       + "        {\n"
                                                       + "            \"location\": {\n"
                                                       + "                \"names\": {\n"
                                                       + "                    \"nwsLid\": \"FROV2\",\n"
                                                       + "                    \"usgsSiteCode\": \"01631000\",\n"
                                                       + "                    \"nwmFeatureId\": 5907079,\n"
                                                       + "                    \"nwsName\": \"Front Royal\",\n"
                                                       + "                    \"usgsName\": \"S F SHENANDOAH RIVER AT FRONT ROYAL, VA\" \n"
                                                       + "                },\n"
                                                       + "                \"nwsCoordinates\": {\n"
                                                       + "                    \"latitude\": 38.913888888889,\n"
                                                       + "                    \"longitude\": -78.211111111111,\n"
                                                       + "                    \"crs\": \"NAD27\",\n"
                                                       + "                    \"link\": \"https://maps.google.com/maps?q=38.913888888889,-78.211111111111\" \n"
                                                       + "                },\n"
                                                       + "                \"usgsCoordinates\": {\n"
                                                       + "                    \"latitude\": 38.91400059,\n"
                                                       + "                    \"longitude\": -78.21083388,\n"
                                                       + "                    \"crs\": \"NAD83\",\n"
                                                       + "                    \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\" \n"
                                                       + "                }\n"
                                                       + "            },\n"
                                                       + "            \"producer\": \"MARFC\",\n"
                                                       + "            \"issuer\": \"CTP\",\n"
                                                       + "            \"distributor\": \"SBN\",\n"
                                                       + "            \"generationTime\": \"2022-03-01T13:39:18Z\",\n"
                                                       + "            \"parameterCodes\": {\n"
                                                       + "                \"physicalElement\": \"QR\",\n"
                                                       + "                \"duration\": \"I\",\n"
                                                       + "                \"typeSource\": \"RG\" \n"
                                                       + "            },\n"
                                                       + "            \"thresholds\": {\n"
                                                       + "                \"units\": \"CFS\",\n"
                                                       + "                \"action\": null,\n"
                                                       + "                \"minor\": 24340.0,\n"
                                                       + "                \"moderate\": 31490.0,\n"
                                                       + "                \"major\": 47200.0,\n"
                                                       + "                \"record\": 130000.0\n"
                                                       + "            },\n"
                                                       + "            \"timeseries\": [\n"
                                                       + "                {\n"
                                                       + "                    \"identifier\": \"1\",\n"
                                                       + "                    \"units\": \"KCFS\",\n"
                                                       + "                    \"dataPoints\": [\n"
                                                       + "                        {\n"
                                                       + "                            \"time\": \"2022-03-01T12:30:00Z\",\n"
                                                       + "                            \"value\": 1.62,\n"
                                                       + "                            \"status\": \"no_flooding\" \n"
                                                       + "                        },\n"
                                                       + "                        {\n"
                                                       + "                            \"time\": \"2022-03-01T12:45:00Z\",\n"
                                                       + "                            \"value\": 1.62,\n"
                                                       + "                            \"status\": \"no_flooding\" \n"
                                                       + "                        },\n"
                                                       + "                        {\n"
                                                       + "                            \"time\": \"2022-03-01T13:00:00Z\",\n"
                                                       + "                            \"value\": 1.62,\n"
                                                       + "                            \"status\": \"no_flooding\" \n"
                                                       + "                        },\n"
                                                       + "                        {\n"
                                                       + "                            \"time\": \"2022-03-01T13:15:00Z\",\n"
                                                       + "                            \"value\": 1.62,\n"
                                                       + "                            \"status\": \"no_flooding\" \n"
                                                       + "                        },\n"
                                                       + "                        {\n"
                                                       + "                            \"time\": \"2022-03-01T13:30:00Z\",\n"
                                                       + "                            \"value\": 1.62,\n"
                                                       + "                            \"status\": \"no_flooding\" \n"
                                                       + "                        },\n"
                                                       + "                        {\n"
                                                       + "                            \"time\": \"2022-03-01T13:45:00Z\",\n"
                                                       + "                            \"value\": 1.62,\n"
                                                       + "                            \"status\": \"no_flooding\" \n"
                                                       + "                        },\n"
                                                       + "                        {\n"
                                                       + "                            \"time\": \"2022-03-01T14:00:00Z\",\n"
                                                       + "                            \"value\": 1.65,\n"
                                                       + "                            \"status\": \"no_flooding\" \n"
                                                       + "                        }\n"
                                                       + "                    ]\n"
                                                       + "                }\n"
                                                       + "            ]\n"
                                                       + "        }\n"
                                                       + "    ]\n"
                                                       + "}\n";

    private static final String ANOTHER_FORECAST_RESPONSE_V2 = "{\n"
                                                               + "  \"_documentation\": {\n"
                                                               + "    \"swagger URL\": \"http://***REMOVED***.***REMOVED***.***REMOVED***/docs/rfc_forecast/v2.0/swagger/\"\n"
                                                               + "  },\n"
                                                               + "  \"deployment\": {\n"
                                                               + "    \"api_url\": \"https://***REMOVED***.***REMOVED***.***REMOVED***/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/FROV2/?format=json&issuedTime=%5B2022-09-17T00%3A00%3A00Z%2C2022-09-19T18%3A57%3A46Z%5D&proj=WRES\",\n"
                                                               + "    \"stack\": \"prod\",\n"
                                                               + "    \"version\": \"v2.5.1\",\n"
                                                               + "    \"api_caller\": \"WRES\"\n"
                                                               + "  },\n"
                                                               + "  \"Non-urgent_issue_reporting_link\": \"https://vlab.***REMOVED***/redmine/projects/wrds-user-support/issues/new?issue[category_id]=2831\",\n"
                                                               + "  \"header\": {\n"
                                                               + "    \"request\": {\n"
                                                               + "      \"params\": {\n"
                                                               + "        \"asProvided\": {\n"
                                                               + "          \"issuedTime\": \"[2022-09-17T00:00:00Z,2022-09-19T18:57:46Z]\"\n"
                                                               + "        },\n"
                                                               + "        \"asUsed\": {\n"
                                                               + "          \"issuedTime\": \"[2022-09-17T00:00:00Z,2022-09-19T18:57:46Z]\",\n"
                                                               + "          \"validTime\": \"all\",\n"
                                                               + "          \"excludePast\": \"False\",\n"
                                                               + "          \"minForecastStatus\": \"no_flooding\",\n"
                                                               + "          \"use_rating_curve\": null,\n"
                                                               + "          \"includeDuplicates\": \"False\",\n"
                                                               + "          \"returnNewestForecast\": \"False\",\n"
                                                               + "          \"completeForecastStatus\": \"False\"\n"
                                                               + "        }\n"
                                                               + "      }\n"
                                                               + "    },\n"
                                                               + "    \"missing_values\": [\n"
                                                               + "      -999,\n"
                                                               + "      -9999\n"
                                                               + "    ]\n"
                                                               + "  },\n"
                                                               + "  \"forecasts\": [\n"
                                                               + "    {\n"
                                                               + "      \"location\": {\n"
                                                               + "        \"names\": {\n"
                                                               + "          \"nwsLid\": \"FROV2\",\n"
                                                               + "          \"usgsSiteCode\": \"01631000\",\n"
                                                               + "          \"nwm_feature_id\": 5907079,\n"
                                                               + "          \"nwsName\": \"Front Royal\",\n"
                                                               + "          \"usgsName\": \"S F SHENANDOAH RIVER AT FRONT ROYAL, VA\"\n"
                                                               + "        },\n"
                                                               + "        \"coordinates\": {\n"
                                                               + "          \"latitude\": 38.91400059,\n"
                                                               + "          \"longitude\": -78.21083388,\n"
                                                               + "          \"crs\": \"NAD83\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                               + "        },\n"
                                                               + "        \"nws_coordinates\": {\n"
                                                               + "          \"latitude\": 38.913888888889,\n"
                                                               + "          \"longitude\": -78.211111111111,\n"
                                                               + "          \"crs\": \"NAD27\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.913888888889,-78.211111111111\"\n"
                                                               + "        },\n"
                                                               + "        \"usgs_coordinates\": {\n"
                                                               + "          \"latitude\": 38.91400059,\n"
                                                               + "          \"longitude\": -78.21083388,\n"
                                                               + "          \"crs\": \"NAD83\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                               + "        }\n"
                                                               + "      },\n"
                                                               + "      \"producer\": \"MARFC\",\n"
                                                               + "      \"issuer\": \"LWX\",\n"
                                                               + "      \"distributor\": \"SBN\",\n"
                                                               + "      \"type\": \"deterministic\",\n"
                                                               + "      \"issuedTime\": \"2022-09-17T13:17:00Z\",\n"
                                                               + "      \"generationTime\": \"2022-09-17T13:24:19Z\",\n"
                                                               + "      \"parameterCodes\": {\n"
                                                               + "        \"physicalElement\": \"QR\",\n"
                                                               + "        \"duration\": \"I\",\n"
                                                               + "        \"typeSource\": \"FF\",\n"
                                                               + "        \"extremum\": \"Z\",\n"
                                                               + "        \"probability\": \"Z\"\n"
                                                               + "      },\n"
                                                               + "      \"thresholds\": {\n"
                                                               + "        \"units\": \"CFS\",\n"
                                                               + "        \"action\": null,\n"
                                                               + "        \"minor\": 24340,\n"
                                                               + "        \"moderate\": 31490,\n"
                                                               + "        \"major\": 47200,\n"
                                                               + "        \"record\": 130000\n"
                                                               + "      },\n"
                                                               + "      \"units\": {\n"
                                                               + "        \"streamflow\": \"KCFS\"\n"
                                                               + "      },\n"
                                                               + "      \"members\": [\n"
                                                               + "        {\n"
                                                               + "          \"identifier\": \"1\",\n"
                                                               + "          \"forecast_status\": \"no_flooding\",\n"
                                                               + "          \"dataPointsList\": [\n"
                                                               + "            [\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-17T18:00:00Z\",\n"
                                                               + "                \"value\": 0.526,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-18T00:00:00Z\",\n"
                                                               + "                \"value\": 0.526,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-18T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-18T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-18T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              }\n"
                                                               + "            ]\n"
                                                               + "          ]\n"
                                                               + "        }\n"
                                                               + "      ]\n"
                                                               + "    },\n"
                                                               + "    {\n"
                                                               + "      \"location\": {\n"
                                                               + "        \"names\": {\n"
                                                               + "          \"nwsLid\": \"FROV2\",\n"
                                                               + "          \"usgsSiteCode\": \"01631000\",\n"
                                                               + "          \"nwm_feature_id\": 5907079,\n"
                                                               + "          \"nwsName\": \"Front Royal\",\n"
                                                               + "          \"usgsName\": \"S F SHENANDOAH RIVER AT FRONT ROYAL, VA\"\n"
                                                               + "        },\n"
                                                               + "        \"coordinates\": {\n"
                                                               + "          \"latitude\": 38.91400059,\n"
                                                               + "          \"longitude\": -78.21083388,\n"
                                                               + "          \"crs\": \"NAD83\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                               + "        },\n"
                                                               + "        \"nws_coordinates\": {\n"
                                                               + "          \"latitude\": 38.913888888889,\n"
                                                               + "          \"longitude\": -78.211111111111,\n"
                                                               + "          \"crs\": \"NAD27\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.913888888889,-78.211111111111\"\n"
                                                               + "        },\n"
                                                               + "        \"usgs_coordinates\": {\n"
                                                               + "          \"latitude\": 38.91400059,\n"
                                                               + "          \"longitude\": -78.21083388,\n"
                                                               + "          \"crs\": \"NAD83\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                               + "        }\n"
                                                               + "      },\n"
                                                               + "      \"producer\": \"MARFC\",\n"
                                                               + "      \"issuer\": \"LWX\",\n"
                                                               + "      \"distributor\": \"SBN\",\n"
                                                               + "      \"type\": \"deterministic\",\n"
                                                               + "      \"issuedTime\": \"2022-09-18T13:15:00Z\",\n"
                                                               + "      \"generationTime\": \"2022-09-18T13:24:19Z\",\n"
                                                               + "      \"parameterCodes\": {\n"
                                                               + "        \"physicalElement\": \"QR\",\n"
                                                               + "        \"duration\": \"I\",\n"
                                                               + "        \"typeSource\": \"FF\",\n"
                                                               + "        \"extremum\": \"Z\",\n"
                                                               + "        \"probability\": \"Z\"\n"
                                                               + "      },\n"
                                                               + "      \"thresholds\": {\n"
                                                               + "        \"units\": \"CFS\",\n"
                                                               + "        \"action\": null,\n"
                                                               + "        \"minor\": 24340,\n"
                                                               + "        \"moderate\": 31490,\n"
                                                               + "        \"major\": 47200,\n"
                                                               + "        \"record\": 130000\n"
                                                               + "      },\n"
                                                               + "      \"units\": {\n"
                                                               + "        \"streamflow\": \"KCFS\"\n"
                                                               + "      },\n"
                                                               + "      \"members\": [\n"
                                                               + "        {\n"
                                                               + "          \"identifier\": \"1\",\n"
                                                               + "          \"forecast_status\": \"no_flooding\",\n"
                                                               + "          \"dataPointsList\": [\n"
                                                               + "            [\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-18T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-21T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-21T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-21T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              }\n"
                                                               + "            ]\n"
                                                               + "          ]\n"
                                                               + "        }\n"
                                                               + "      ]\n"
                                                               + "    },\n"
                                                               + "    {\n"
                                                               + "      \"location\": {\n"
                                                               + "        \"names\": {\n"
                                                               + "          \"nwsLid\": \"FROV2\",\n"
                                                               + "          \"usgsSiteCode\": \"01631000\",\n"
                                                               + "          \"nwm_feature_id\": 5907079,\n"
                                                               + "          \"nwsName\": \"Front Royal\",\n"
                                                               + "          \"usgsName\": \"S F SHENANDOAH RIVER AT FRONT ROYAL, VA\"\n"
                                                               + "        },\n"
                                                               + "        \"coordinates\": {\n"
                                                               + "          \"latitude\": 38.91400059,\n"
                                                               + "          \"longitude\": -78.21083388,\n"
                                                               + "          \"crs\": \"NAD83\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                               + "        },\n"
                                                               + "        \"nws_coordinates\": {\n"
                                                               + "          \"latitude\": 38.913888888889,\n"
                                                               + "          \"longitude\": -78.211111111111,\n"
                                                               + "          \"crs\": \"NAD27\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.913888888889,-78.211111111111\"\n"
                                                               + "        },\n"
                                                               + "        \"usgs_coordinates\": {\n"
                                                               + "          \"latitude\": 38.91400059,\n"
                                                               + "          \"longitude\": -78.21083388,\n"
                                                               + "          \"crs\": \"NAD83\",\n"
                                                               + "          \"link\": \"https://maps.google.com/maps?q=38.91400059,-78.21083388\"\n"
                                                               + "        }\n"
                                                               + "      },\n"
                                                               + "      \"producer\": \"MARFC\",\n"
                                                               + "      \"issuer\": \"LWX\",\n"
                                                               + "      \"distributor\": \"SBN\",\n"
                                                               + "      \"type\": \"deterministic\",\n"
                                                               + "      \"issuedTime\": \"2022-09-19T13:41:00Z\",\n"
                                                               + "      \"generationTime\": \"2022-09-19T13:54:21Z\",\n"
                                                               + "      \"parameterCodes\": {\n"
                                                               + "        \"physicalElement\": \"QR\",\n"
                                                               + "        \"duration\": \"I\",\n"
                                                               + "        \"typeSource\": \"FF\",\n"
                                                               + "        \"extremum\": \"Z\",\n"
                                                               + "        \"probability\": \"Z\"\n"
                                                               + "      },\n"
                                                               + "      \"thresholds\": {\n"
                                                               + "        \"units\": \"CFS\",\n"
                                                               + "        \"action\": null,\n"
                                                               + "        \"minor\": 24340,\n"
                                                               + "        \"moderate\": 31490,\n"
                                                               + "        \"major\": 47200,\n"
                                                               + "        \"record\": 130000\n"
                                                               + "      },\n"
                                                               + "      \"units\": {\n"
                                                               + "        \"streamflow\": \"KCFS\"\n"
                                                               + "      },\n"
                                                               + "      \"members\": [\n"
                                                               + "        {\n"
                                                               + "          \"identifier\": \"1\",\n"
                                                               + "          \"forecast_status\": \"no_flooding\",\n"
                                                               + "          \"dataPointsList\": [\n"
                                                               + "            [\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-19T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-20T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-21T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-21T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-21T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-21T18:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-22T00:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-22T06:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              },\n"
                                                               + "              {\n"
                                                               + "                \"time\": \"2022-09-22T12:00:00Z\",\n"
                                                               + "                \"value\": 0.431,\n"
                                                               + "                \"status\": \"no_flooding\"\n"
                                                               + "              }\n"
                                                               + "            ]\n"
                                                               + "          ]\n"
                                                               + "        }\n"
                                                               + "      ]\n"
                                                               + "    }\n"
                                                               + "  ]\n"
                                                               + "}\n";

    private static final String GET = "GET";

    @BeforeEach
    void startServer()
    {
        this.mockServer = ClientAndServer.startClientAndServer( 0 );
    }

    @AfterEach
    void stopServer()
    {
        this.mockServer.stop();
    }

    @Test
    void testReadReturnsOneForecastTimeSeries()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( FORECAST_PATH_V2 )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( FORECAST_RESPONSE_V2 ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + FORECAST_PATH_V2 );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_AHPS,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( null,
                                                                     List.of( fakeDeclarationSource ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               fakeUri,
                                               LeftOrRightOrBaseline.RIGHT );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsAhpsReader reader = WrdsAhpsReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Geometry geometry = MessageFactory.getGeometry( FEATURE_NAME,
                                                            "Front Royal",
                                                            null,
                                                            null );

            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ISSUED_TIME,
                                                                         Instant.parse( "2021-11-14T13:46:00Z" ) ),
                                                                 TimeScaleOuter.of(),
                                                                 "QR",
                                                                 Feature.of( geometry ),
                                                                 "KCFS" );
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2021-11-14T18:00:00Z" ),
                                                                         2.12 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-15T00:00:00Z" ),
                                                                         2.12 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-15T06:00:00Z" ),
                                                                         2.01 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-15T12:00:00Z" ),
                                                                         1.89 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-15T18:00:00Z" ),
                                                                         1.78 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-16T00:00:00Z" ),
                                                                         1.67 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-16T06:00:00Z" ),
                                                                         1.67 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-16T12:00:00Z" ),
                                                                         1.52 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-16T18:00:00Z" ),
                                                                         1.52 ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-17T00:00:00Z" ),
                                                                         Double.NaN ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-17T06:00:00Z" ),
                                                                         Double.NaN ) )
                                                    .addEvent( Event.of( Instant.parse( "2021-11-17T12:00:00Z" ),
                                                                         1.38 ) )
                                                    .setMetadata( metadata )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadReturnsThreeForecastTimeSeries()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( FORECAST_PATH_V2 )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( ANOTHER_FORECAST_RESPONSE_V2 ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + FORECAST_PATH_V2 );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_AHPS,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( null,
                                                                     List.of( fakeDeclarationSource ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               fakeUri,
                                               LeftOrRightOrBaseline.RIGHT );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsAhpsReader reader = WrdsAhpsReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            assertEquals( 3, actual.size() );
        }
    }

    @Test
    void testReadReturnsThreeForecastTimeSeriesInOneChunk()
    {
        // Create the chunk parameters
        Parameters parameters = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                new Parameter( "issuedTime",
                                                               "[2022-09-17T00:00:00Z,2022-09-19T18:57:46Z]" ) );

        String path = "/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/FROV2";
        
        this.mockServer.when( HttpRequest.request()
                                         .withPath( path )
                                         .withQueryStringParameters( parameters )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( ANOTHER_FORECAST_RESPONSE_V2 ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + "/api/rfc_forecast/v2.0/forecast/streamflow/" );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_AHPS,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "QR", null ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               fakeUri,
                                               LeftOrRightOrBaseline.RIGHT );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                List.of( new NamedFeature( null, FEATURE_NAME, null ) ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                new DateCondition( "2022-09-17T00:00:00Z", "2022-09-19T18:57:46Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsAhpsReader reader = WrdsAhpsReader.of( pairConfig, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // One request made with parameters
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( path )
                                         .withQueryStringParameters( parameters ),
                                VerificationTimes.exactly( 1 ) );
    }

    @Test
    void testReadReturnsThreeChunkedObservedTimeSeries()
    {
        // Create the chunk parameters
        Parameters parametersOne = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                   new Parameter( "validTime",
                                                                  "[2018-01-01T00:00:00Z,2019-01-01T00:00:00Z]" ) );

        Parameters parametersTwo = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                   new Parameter( "validTime",
                                                                  "[2019-01-01T00:00:00Z,2020-01-01T00:00:00Z]" ) );

        Parameters parametersThree = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                     new Parameter( "validTime",
                                                                    "[2020-01-01T00:00:00Z,2021-01-01T00:00:00Z]" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( OBSERVED_PATH_V1 + FEATURE_NAME )
                                         .withQueryStringParameters( parametersOne )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( OBSERVED_RESPONSE_V1 ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( OBSERVED_PATH_V1 + FEATURE_NAME )
                                         .withQueryStringParameters( parametersTwo )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( OBSERVED_RESPONSE_V1 ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( OBSERVED_PATH_V1 + FEATURE_NAME )
                                         .withQueryStringParameters( parametersThree )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( OBSERVED_RESPONSE_V1 ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + OBSERVED_PATH_V1
                                  + OBSERVED_PATH_PARAMS_V1 );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_OBS,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( DatasourceType.OBSERVATIONS,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "QR", null ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               fakeUri,
                                               LeftOrRightOrBaseline.LEFT );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                List.of( new NamedFeature( FEATURE_NAME, null, null ) ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                new DateCondition( "2018-01-01T00:00:00Z", "2021-01-01T00:00:00Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsAhpsReader reader = WrdsAhpsReader.of( pairConfig, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // Three requests made
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( OBSERVED_PATH_V1 + FEATURE_NAME ),
                                VerificationTimes.exactly( 3 ) );

        // One request made with parameters one
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( OBSERVED_PATH_V1 + FEATURE_NAME )
                                         .withQueryStringParameters( parametersOne ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters two
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( OBSERVED_PATH_V1 + FEATURE_NAME )
                                         .withQueryStringParameters( parametersTwo ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters three
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( OBSERVED_PATH_V1 + FEATURE_NAME )
                                         .withQueryStringParameters( parametersThree ),
                                VerificationTimes.exactly( 1 ) );

    }

}
