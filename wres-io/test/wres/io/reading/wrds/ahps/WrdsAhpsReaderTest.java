package wres.io.reading.wrds.ahps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
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
    private static final String FORECAST_RESPONSE_V2 = """
            {
                "_documentation": {
                    "swagger URL": "http://fake.wrds.gov/docs/rfc_forecast/v2.0/swagger/"
                },
                "deployment": {
                    "api_url": "https://fake.wrds.gov/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/FROV2/",
                    "stack": "prod",
                    "version": "v2.4.0",
                    "api_caller": "None"
                },
                "_metrics": {
                    "forecast_count": 1,
                    "location_count": 1,
                    "total_request_time": 1.1423799991607666
                },
                "header": {
                    "request": {
                        "params": {
                            "asProvided": {},
                            "asUsed": {
                                "issuedTime": "latest",
                                "validTime": "all",
                                "excludePast": "False",
                                "minForecastStatus": "no_flooding",
                                "use_rating_curve": null,
                                "includeDuplicates": "False",
                                "returnNewestForecast": "False",
                                "completeForecastStatus": "False"
                            }
                        }
                    },
                    "missing_values": [
                        -999,
                        -9999
                    ]
                },
                "forecasts": [
                    {
                        "location": {
                            "names": {
                                "nwsLid": "FROV2",
                                "usgsSiteCode": "01631000",
                                "nwm_feature_id": 5907079,
                                "nwsName": "Front Royal",
                                "usgsName": "S F SHENANDOAH RIVER AT FRONT ROYAL, VA"
                            },
                            "coordinates": {
                                "latitude": 38.91400059,
                                "longitude": -78.21083388,
                                "crs": "NAD83",
                                "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                            },
                            "nws_coordinates": {
                                "latitude": 38.913888888889,
                                "longitude": -78.211111111111,
                                "crs": "NAD27",
                                "link": "https://maps.google.com/maps?q=38.913888888889,-78.211111111111"
                            },
                            "usgs_coordinates": {
                                "latitude": 38.91400059,
                                "longitude": -78.21083388,
                                "crs": "NAD83",
                                "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                            }
                        },
                        "producer": "MARFC",
                        "issuer": "LWX",
                        "distributor": "SBN",
                        "type": "deterministic",
                        "issuedTime": "2021-11-14T13:46:00Z",
                        "generationTime": "2021-11-14T13:54:18Z",
                        "parameterCodes": {
                            "physicalElement": "QR",
                            "duration": "I",
                            "typeSource": "FF",
                            "extremum": "Z",
                            "probability": "Z"
                        },
                        "thresholds": {
                            "action": null,
                            "minor": 24340.0,
                            "moderate": 31490.0,
                            "major": 47200.0,
                            "record": 130000.0
                        },
                        "units": {
                            "streamflow": "KCFS"
                        },
                        "members": [
                            {
                                "identifier": "1",
                                "forecast_status": "no_flooding",
                                "dataPointsList": [
                                    [
                                        {
                                            "time": "2021-11-14T18:00:00Z",
                                            "value": 2.12,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-15T00:00:00Z",
                                            "value": 2.12,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-15T06:00:00Z",
                                            "value": 2.01,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-15T12:00:00Z",
                                            "value": 1.89,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-15T18:00:00Z",
                                            "value": 1.78,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-16T00:00:00Z",
                                            "value": 1.67,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-16T06:00:00Z",
                                            "value": 1.67,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-16T12:00:00Z",
                                            "value": 1.52,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-16T18:00:00Z",
                                            "value": 1.52,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-17T00:00:00Z",
                                            "value":  -9999.00,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-17T06:00:00Z",
                                            "value": -999,
                                            "status": "no_flooding"
                                        },
                                        {
                                            "time": "2021-11-17T12:00:00Z",
                                            "value": 1.38,
                                            "status": "no_flooding"
                                        }
                                    ]
                                ]
                            }
                        ]
                    }
                ]
            }
            """;

    /** Path used by GET for observations. */
    private static final String OBSERVED_PATH_V1 =
            "/api/observed/v1.0/observed/streamflow/nws_lid/";

    /** Path parameters used by GET for observations. */
    private static final String OBSERVED_PATH_PARAMS_V1 =
            "?proj=WRES&validTime=%5B2022-03-01T00%3A00%3A00Z%2C2022-03-01T14%3A12%3A59Z%5D";

    /** Observed response from GET. */
    private static final String OBSERVED_RESPONSE_V1 = """
            {
                "_documentation": {
                    "swaggerURL": "http://fake.wrds.gov/docs/observed/v1.0/swagger/"\s
                },
                "_deployment": {
                    "apiUrl":"https://fake.wrds.gov/api/observed/v1.0/observed/streamflow/nws_lid/FROV2/?proj=WRES&validTime=%5B2022-03-01T00%3A00%3A00Z%2C2022-03-01T14%3A12%3A59Z%5D",
                    "stack": "dev",
                    "version": "v1.0.0",
                    "apiCaller": "WRES"\s
                },
                "_nonUrgentIssueReportingLink": "https://fake.gov/redmine/projects/wrds-user-support/issues/new?issue[category_id]=2835",
                "_metrics": {
                    "observedDataCount": 64,
                    "locationCount": 2,
                    "totalRequestTime": 0.6576216220855713
                },
                "header": {
                    "request": {
                        "params": {
                            "asProvided": {
                                "validTime": "[2022-03-01T00:00:00Z,2022-03-01T14:12:59Z]"\s
                            },
                            "asUsed": {
                                "validTime": "[2022-03-01T00:00:00Z,2022-03-01T14:12:59Z]",
                                "nonUSGSGages": false,
                                "pe": null,
                                "ts": null
                            }
                        }
                    },
                    "missingValues": [
                        -999,
                        -9999
                    ]
                },
                "timeseriesDataset": [
                    {
                        "location": {
                            "names": {
                                "nwsLid": "FROV2",
                                "usgsSiteCode": "01631000",
                                "nwmFeatureId": 5907079,
                                "nwsName": "Front Royal",
                                "usgsName": "S F SHENANDOAH RIVER AT FRONT ROYAL, VA"\s
                            },
                            "nwsCoordinates": {
                                "latitude": 38.913888888889,
                                "longitude": -78.211111111111,
                                "crs": "NAD27",
                                "link": "https://maps.google.com/maps?q=38.913888888889,-78.211111111111"\s
                            },
                            "usgsCoordinates": {
                                "latitude": 38.91400059,
                                "longitude": -78.21083388,
                                "crs": "NAD83",
                                "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"\s
                            }
                        },
                        "producer": "MARFC",
                        "issuer": "CTP",
                        "distributor": "SBN",
                        "generationTime": "2022-03-01T13:39:18Z",
                        "parameterCodes": {
                            "physicalElement": "QR",
                            "duration": "I",
                            "typeSource": "RG"\s
                        },
                        "thresholds": {
                            "units": "CFS",
                            "action": null,
                            "minor": 24340.0,
                            "moderate": 31490.0,
                            "major": 47200.0,
                            "record": 130000.0
                        },
                        "timeseries": [
                            {
                                "identifier": "1",
                                "units": "KCFS",
                                "dataPoints": [
                                    {
                                        "time": "2022-03-01T12:30:00Z",
                                        "value": 1.62,
                                        "status": "no_flooding"\s
                                    },
                                    {
                                        "time": "2022-03-01T12:45:00Z",
                                        "value": 1.62,
                                        "status": "no_flooding"\s
                                    },
                                    {
                                        "time": "2022-03-01T13:00:00Z",
                                        "value": 1.62,
                                        "status": "no_flooding"\s
                                    },
                                    {
                                        "time": "2022-03-01T13:15:00Z",
                                        "value": 1.62,
                                        "status": "no_flooding"\s
                                    },
                                    {
                                        "time": "2022-03-01T13:30:00Z",
                                        "value": 1.62,
                                        "status": "no_flooding"\s
                                    },
                                    {
                                        "time": "2022-03-01T13:45:00Z",
                                        "value": 1.62,
                                        "status": "no_flooding"\s
                                    },
                                    {
                                        "time": "2022-03-01T14:00:00Z",
                                        "value": 1.65,
                                        "status": "no_flooding"\s
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
            """;

    private static final String ANOTHER_FORECAST_RESPONSE_V2 = """
            {
              "_documentation": {
                "swagger URL": "http://fake.wrds.gov/docs/rfc_forecast/v2.0/swagger/"
              },
              "deployment": {
                "api_url": "https://fake.wrds.gov/api/rfc_forecast/v2.0/forecast/streamflow/nws_lid/FROV2/?format=json&issuedTime=%5B2022-09-17T00%3A00%3A00Z%2C2022-09-19T18%3A57%3A46Z%5D&proj=WRES",
                "stack": "prod",
                "version": "v2.5.1",
                "api_caller": "WRES"
              },
              "Non-urgent_issue_reporting_link": "https://fake.gov/redmine/projects/wrds-user-support/issues/new?issue[category_id]=2831",
              "header": {
                "request": {
                  "params": {
                    "asProvided": {
                      "issuedTime": "[2022-09-17T00:00:00Z,2022-09-19T18:57:46Z]"
                    },
                    "asUsed": {
                      "issuedTime": "[2022-09-17T00:00:00Z,2022-09-19T18:57:46Z]",
                      "validTime": "all",
                      "excludePast": "False",
                      "minForecastStatus": "no_flooding",
                      "use_rating_curve": null,
                      "includeDuplicates": "False",
                      "returnNewestForecast": "False",
                      "completeForecastStatus": "False"
                    }
                  }
                },
                "missing_values": [
                  -999,
                  -9999
                ]
              },
              "forecasts": [
                {
                  "location": {
                    "names": {
                      "nwsLid": "FROV2",
                      "usgsSiteCode": "01631000",
                      "nwm_feature_id": 5907079,
                      "nwsName": "Front Royal",
                      "usgsName": "S F SHENANDOAH RIVER AT FRONT ROYAL, VA"
                    },
                    "coordinates": {
                      "latitude": 38.91400059,
                      "longitude": -78.21083388,
                      "crs": "NAD83",
                      "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                    },
                    "nws_coordinates": {
                      "latitude": 38.913888888889,
                      "longitude": -78.211111111111,
                      "crs": "NAD27",
                      "link": "https://maps.google.com/maps?q=38.913888888889,-78.211111111111"
                    },
                    "usgs_coordinates": {
                      "latitude": 38.91400059,
                      "longitude": -78.21083388,
                      "crs": "NAD83",
                      "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                    }
                  },
                  "producer": "MARFC",
                  "issuer": "LWX",
                  "distributor": "SBN",
                  "type": "deterministic",
                  "issuedTime": "2022-09-17T13:17:00Z",
                  "generationTime": "2022-09-17T13:24:19Z",
                  "parameterCodes": {
                    "physicalElement": "QR",
                    "duration": "I",
                    "typeSource": "FF",
                    "extremum": "Z",
                    "probability": "Z"
                  },
                  "thresholds": {
                    "units": "CFS",
                    "action": null,
                    "minor": 24340,
                    "moderate": 31490,
                    "major": 47200,
                    "record": 130000
                  },
                  "units": {
                    "streamflow": "KCFS"
                  },
                  "members": [
                    {
                      "identifier": "1",
                      "forecast_status": "no_flooding",
                      "dataPointsList": [
                        [
                          {
                            "time": "2022-09-17T18:00:00Z",
                            "value": 0.526,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-18T00:00:00Z",
                            "value": 0.526,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-18T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-18T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-18T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          }
                        ]
                      ]
                    }
                  ]
                },
                {
                  "location": {
                    "names": {
                      "nwsLid": "FROV2",
                      "usgsSiteCode": "01631000",
                      "nwm_feature_id": 5907079,
                      "nwsName": "Front Royal",
                      "usgsName": "S F SHENANDOAH RIVER AT FRONT ROYAL, VA"
                    },
                    "coordinates": {
                      "latitude": 38.91400059,
                      "longitude": -78.21083388,
                      "crs": "NAD83",
                      "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                    },
                    "nws_coordinates": {
                      "latitude": 38.913888888889,
                      "longitude": -78.211111111111,
                      "crs": "NAD27",
                      "link": "https://maps.google.com/maps?q=38.913888888889,-78.211111111111"
                    },
                    "usgs_coordinates": {
                      "latitude": 38.91400059,
                      "longitude": -78.21083388,
                      "crs": "NAD83",
                      "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                    }
                  },
                  "producer": "MARFC",
                  "issuer": "LWX",
                  "distributor": "SBN",
                  "type": "deterministic",
                  "issuedTime": "2022-09-18T13:15:00Z",
                  "generationTime": "2022-09-18T13:24:19Z",
                  "parameterCodes": {
                    "physicalElement": "QR",
                    "duration": "I",
                    "typeSource": "FF",
                    "extremum": "Z",
                    "probability": "Z"
                  },
                  "thresholds": {
                    "units": "CFS",
                    "action": null,
                    "minor": 24340,
                    "moderate": 31490,
                    "major": 47200,
                    "record": 130000
                  },
                  "units": {
                    "streamflow": "KCFS"
                  },
                  "members": [
                    {
                      "identifier": "1",
                      "forecast_status": "no_flooding",
                      "dataPointsList": [
                        [
                          {
                            "time": "2022-09-18T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-19T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-21T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-21T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-21T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          }
                        ]
                      ]
                    }
                  ]
                },
                {
                  "location": {
                    "names": {
                      "nwsLid": "FROV2",
                      "usgsSiteCode": "01631000",
                      "nwm_feature_id": 5907079,
                      "nwsName": "Front Royal",
                      "usgsName": "S F SHENANDOAH RIVER AT FRONT ROYAL, VA"
                    },
                    "coordinates": {
                      "latitude": 38.91400059,
                      "longitude": -78.21083388,
                      "crs": "NAD83",
                      "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                    },
                    "nws_coordinates": {
                      "latitude": 38.913888888889,
                      "longitude": -78.211111111111,
                      "crs": "NAD27",
                      "link": "https://maps.google.com/maps?q=38.913888888889,-78.211111111111"
                    },
                    "usgs_coordinates": {
                      "latitude": 38.91400059,
                      "longitude": -78.21083388,
                      "crs": "NAD83",
                      "link": "https://maps.google.com/maps?q=38.91400059,-78.21083388"
                    }
                  },
                  "producer": "MARFC",
                  "issuer": "LWX",
                  "distributor": "SBN",
                  "type": "deterministic",
                  "issuedTime": "2022-09-19T13:41:00Z",
                  "generationTime": "2022-09-19T13:54:21Z",
                  "parameterCodes": {
                    "physicalElement": "QR",
                    "duration": "I",
                    "typeSource": "FF",
                    "extremum": "Z",
                    "probability": "Z"
                  },
                  "thresholds": {
                    "units": "CFS",
                    "action": null,
                    "minor": 24340,
                    "moderate": 31490,
                    "major": 47200,
                    "record": 130000
                  },
                  "units": {
                    "streamflow": "KCFS"
                  },
                  "members": [
                    {
                      "identifier": "1",
                      "forecast_status": "no_flooding",
                      "dataPointsList": [
                        [
                          {
                            "time": "2022-09-19T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-20T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-21T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-21T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-21T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-21T18:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-22T00:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-22T06:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          },
                          {
                            "time": "2022-09-22T12:00:00Z",
                            "value": 0.431,
                            "status": "no_flooding"
                          }
                        ]
                      ]
                    }
                  ]
                }
              ]
            }
            """;

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

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_AHPS )
                                                    .build();

        Dataset fakeDataset = DatasetBuilder.builder()
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               fakeDataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.RIGHT );

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

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_AHPS )
                                                    .build();

        Dataset fakeDataset = DatasetBuilder.builder()
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               fakeDataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.RIGHT );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsAhpsReader reader = WrdsAhpsReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

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

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_AHPS )
                                                    .build();

        Dataset fakeDataset = DatasetBuilder.builder()
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               fakeDataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.RIGHT );

        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2022-09-17T00:00:00Z" ) )
                                                   .maximum( Instant.parse( "2022-09-19T18:57:46Z" ) )
                                                   .build();
        Geometry geometry = Geometry.newBuilder()
                                    .setName( FEATURE_NAME )
                                    .build();
        GeometryTuple geometryTuple = GeometryTuple.newBuilder()
                                                   .setRight( geometry )
                                                   .build();
        Features features = new Features( Set.of( geometryTuple ) );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .validDates( interval )
                                            .features( features )
                                            .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsAhpsReader reader = WrdsAhpsReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

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

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_OBS )
                                                    .build();

        Dataset fakeDataset = DatasetBuilder.builder()
                                            .type( DataType.OBSERVATIONS )
                                            .variable( VariableBuilder.builder()
                                                                      .name( "QR" )
                                                                      .build() )
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               fakeDataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.LEFT );

        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( Instant.parse( "2018-01-01T00:00:00Z" ) )
                                                   .maximum( Instant.parse( "2021-01-01T00:00:00Z" ) )
                                                   .build();
        Geometry geometry = Geometry.newBuilder()
                                    .setName( FEATURE_NAME )
                                    .build();
        GeometryTuple geometryTuple = GeometryTuple.newBuilder()
                                                   .setLeft( geometry )
                                                   .build();
        Features features = new Features( Set.of( geometryTuple ) );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .validDates( interval )
                                            .features( features )
                                            .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsAhpsReader reader = WrdsAhpsReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

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
