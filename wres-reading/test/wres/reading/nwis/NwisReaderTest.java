package wres.reading.nwis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockserver.model.HttpRequest.request;

import java.net.URI;
import java.time.Duration;
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

import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.reading.DataSource.DataDisposition;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.system.SystemSettings;

/**
 * Tests the {@link NwisReader}.
 * @author James Brown
 */

class NwisReaderTest
{
    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Path used by GET. */
    private static final String PATH = "/nwis/iv/";

    /** Parameters used by GET. */
    private static final String PARAMS =
            "?format=json&indent=on&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:00Z&endDT=2018-10-01T01:00:00Z";

    /** Response from GET. */
    private static final String RESPONSE = """
            {"name" : "ns1:timeSeriesResponseType",\r
            "declaredType" : "org.cuahsi.waterml.TimeSeriesResponseType",\r
            "scope" : "javax.xml.bind.JAXBElement$GlobalScope",\r
            "value" : {\r
              "queryInfo" : {\r
                "queryURL" : "http://nwis.waterservices.usgs.gov/nwis/iv/format=json&indent=on&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:00Z&endDT=2018-10-01T01:00:00Z",\r
                "criteria" : {\r
                  "locationParam" : "[ALL:09165000]",\r
                  "variableParam" : "[00060]",\r
                  "timeParam" : {\r
                    "beginDateTime" : "2018-10-01T00:00:00.000",\r
                    "endDateTime" : "2018-10-01T01:00:00.000"\r
                  },\r
                  "parameter" : [ ]\r
                },\r
                "note" : [ {\r
                  "value" : "[ALL:09165000]",\r
                  "title" : "filter:sites"\r
                }, {\r
                  "value" : "[mode=RANGE, modifiedSince=null] interval={INTERVAL[2018-10-01T00:00:00.000Z/2018-10-01T01:00:00.000Z]}",\r
                  "title" : "filter:timeRange"\r
                }, {\r
                  "value" : "methodIds=[ALL]",\r
                  "title" : "filter:methodId"\r
                }, {\r
                  "value" : "2022-08-08T21:34:38.616Z",\r
                  "title" : "requestDT"\r
                }, {\r
                  "value" : "e7c64270-1761-11ed-911a-4cd98f86fad9",\r
                  "title" : "requestId"\r
                }, {\r
                  "value" : "Provisional data are subject to revision. Go to http://waterdata.usgs.gov/nwis/help/?provisional for more information.",\r
                  "title" : "disclaimer"\r
                }, {\r
                  "value" : "nadww02",\r
                  "title" : "server"\r
                } ]\r
              },\r
              "timeSeries" : [ {\r
                "sourceInfo" : {\r
                  "siteName" : "DOLORES RIVER BELOW RICO, CO.",\r
                  "siteCode" : [ {\r
                    "value" : "09165000",\r
                    "network" : "NWIS",\r
                    "agencyCode" : "USGS"\r
                  } ],\r
                  "timeZoneInfo" : {\r
                    "defaultTimeZone" : {\r
                      "zoneOffset" : "-07:00",\r
                      "zoneAbbreviation" : "MST"\r
                    },\r
                    "daylightSavingsTimeZone" : {\r
                      "zoneOffset" : "-06:00",\r
                      "zoneAbbreviation" : "MDT"\r
                    },\r
                    "siteUsesDaylightSavingsTime" : true\r
                  },\r
                  "geoLocation" : {\r
                    "geogLocation" : {\r
                      "srs" : "EPSG:4326",\r
                      "latitude" : 37.63888428,\r
                      "longitude" : -108.0603517\r
                    },\r
                    "localSiteXY" : [ ]\r
                  },\r
                  "note" : [ ],\r
                  "siteType" : [ ],\r
                  "siteProperty" : [ {\r
                    "value" : "ST",\r
                    "name" : "siteTypeCd"\r
                  }, {\r
                    "value" : "14030002",\r
                    "name" : "hucCd"\r
                  }, {\r
                    "value" : "08",\r
                    "name" : "stateCd"\r
                  }, {\r
                    "value" : "08033",\r
                    "name" : "countyCd"\r
                  } ]\r
                },\r
                "variable" : {\r
                  "variableCode" : [ {\r
                    "value" : "00060",\r
                    "network" : "NWIS",\r
                    "vocabulary" : "NWIS:UnitValues",\r
                    "variableID" : 45807197,\r
                    "default" : true\r
                  } ],\r
                  "variableName" : "Streamflow, ft&#179;/s",\r
                  "variableDescription" : "Discharge, cubic feet per second",\r
                  "valueType" : "Derived Value",\r
                  "unit" : {\r
                    "unitCode" : "ft3/s"\r
                  },\r
                  "options" : {\r
                    "option" : [ {\r
                      "name" : "Statistic",\r
                      "optionCode" : "00000"\r
                    } ]\r
                  },\r
                  "note" : [ ],\r
                  "noDataValue" : -999999.0,\r
                  "variableProperty" : [ ],\r
                  "oid" : "45807197"\r
                },\r
                "values" : [ {\r
                  "value" : [ {\r
                    "value" : "11.0",\r
                    "qualifiers" : [ "A" ],\r
                    "dateTime" : "2018-09-30T18:00:00.000-06:00"\r
                  }, {\r
                    "value" : "11.0",\r
                    "qualifiers" : [ "A" ],\r
                    "dateTime" : "2018-09-30T18:15:00.000-06:00"\r
                  }, {\r
                    "value" : "11.0",\r
                    "qualifiers" : [ "A" ],\r
                    "dateTime" : "2018-09-30T18:30:00.000-06:00"\r
                  }, {\r
                    "value" : "11.0",\r
                    "qualifiers" : [ "A" ],\r
                    "dateTime" : "2018-09-30T18:45:00.000-06:00"\r
                  }, {\r
                    "value" : "11.5",\r
                    "qualifiers" : [ "A" ],\r
                    "dateTime" : "2018-09-30T19:00:00.000-06:00"\r
                  } ],\r
                  "qualifier" : [ {\r
                    "qualifierCode" : "A",\r
                    "qualifierDescription" : "Approved for publication -- Processing and review completed.",\r
                    "qualifierID" : 0,\r
                    "network" : "NWIS",\r
                    "vocabulary" : "uv_rmk_cd"\r
                  } ],\r
                  "qualityControlLevel" : [ ],\r
                  "method" : [ {\r
                    "methodDescription" : "",\r
                    "methodID" : 211948\r
                  } ],\r
                  "source" : [ ],\r
                  "offset" : [ ],\r
                  "sample" : [ ],\r
                  "censorCode" : [ ]\r
                } ],\r
                "name" : "USGS:09165000:00060:00000"\r
              } ]\r
            },\r
            "nil" : false,\r
            "globalScope" : true,\r
            "typeSubstituted" : false\r
            }""";

    /** Parameters used by GET for a multivariate response. */
    private static final String MULTIVARIATE_PARAMS =
            "?format=json&parameterCd=00060,00065&sites=16211600&startDT=2023-01-01T00%3A00%3A01Z&endDT=2023-01-01T01%3A00%3A00Z";

    /** Multivariate response from GET. */
    private static final String MULTIVARIATE_RESPONSE = """
            {
              "name": "ns1:timeSeriesResponseType",
              "declaredType": "org.cuahsi.waterml.TimeSeriesResponseType",
              "scope": "javax.xml.bind.JAXBElement$GlobalScope",
              "value": {
                "queryInfo": {
                  "queryURL": "http://nwis.waterservices.usgs.gov/nwis/ivformat=json&parameterCd=00060,00065&sites=16211600&startDT=2023-01-01T00%3A00%3A01Z&endDT=2023-01-01T01%3A00%3A00Z",
                  "criteria": {
                    "locationParam": "[ALL:16211600]",
                    "variableParam": "[00060, 00065]",
                    "timeParam": {
                      "beginDateTime": "2023-01-01T00:00:01.000",
                      "endDateTime": "2023-01-01T01:00:00.000"
                    },
                    "parameter": []
                  },
                  "note": [
                    {
                      "value": "[ALL:16211600]",
                      "title": "filter:sites"
                    },
                    {
                      "value": "[mode=RANGE, modifiedSince=null] interval={INTERVAL[2023-01-01T00:00:01.000Z/2023-01-01T01:00:00.000Z]}",
                      "title": "filter:timeRange"
                    },
                    {
                      "value": "methodIds=[ALL]",
                      "title": "filter:methodId"
                    },
                    {
                      "value": "2024-06-03T11:00:43.985Z",
                      "title": "requestDT"
                    },
                    {
                      "value": "8611e500-2198-11ef-9a93-4cd98f86fad9",
                      "title": "requestId"
                    },
                    {
                      "value": "Provisional data are subject to revision. Go to http://waterdata.usgs.gov/nwis/help/?provisional for more information.",
                      "title": "disclaimer"
                    },
                    {
                      "value": "nadww02",
                      "title": "server"
                    }
                  ]
                },
                "timeSeries": [
                  {
                    "sourceInfo": {
                      "siteName": "Makaha Str nr Makaha, Oahu, HI",
                      "siteCode": [
                        {
                          "value": "16211600",
                          "network": "NWIS",
                          "agencyCode": "USGS"
                        }
                      ],
                      "timeZoneInfo": {
                        "defaultTimeZone": {
                          "zoneOffset": "-10:00",
                          "zoneAbbreviation": "HST"
                        },
                        "daylightSavingsTimeZone": {
                          "zoneOffset": "-09:00",
                          "zoneAbbreviation": "HDT"
                        },
                        "siteUsesDaylightSavingsTime": false
                      },
                      "geoLocation": {
                        "geogLocation": {
                          "srs": "EPSG:4326",
                          "latitude": 21.50158333,
                          "longitude": -158.1801667
                        },
                        "localSiteXY": []
                      },
                      "note": [],
                      "siteType": [],
                      "siteProperty": [
                        {
                          "value": "ST",
                          "name": "siteTypeCd"
                        },
                        {
                          "value": "20060000",
                          "name": "hucCd"
                        },
                        {
                          "value": "15",
                          "name": "stateCd"
                        },
                        {
                          "value": "15003",
                          "name": "countyCd"
                        }
                      ]
                    },
                    "variable": {
                      "variableCode": [
                        {
                          "value": "00060",
                          "network": "NWIS",
                          "vocabulary": "NWIS:UnitValues",
                          "variableID": 45807197,
                          "default": true
                        }
                      ],
                      "variableName": "Streamflow, ft&#179;/s",
                      "variableDescription": "Discharge, cubic feet per second",
                      "valueType": "Derived Value",
                      "unit": {
                        "unitCode": "ft3/s"
                      },
                      "options": {
                        "option": [
                          {
                            "name": "Statistic",
                            "optionCode": "00000"
                          }
                        ]
                      },
                      "note": [],
                      "noDataValue": -999999,
                      "variableProperty": [],
                      "oid": "45807197"
                    },
                    "values": [
                      {
                        "value": [
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:05:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:10:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:15:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:20:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:25:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:30:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:35:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:40:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:45:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:50:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:55:00.000-10:00"
                          },
                          {
                            "value": "0.00",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T15:00:00.000-10:00"
                          }
                        ],
                        "qualifier": [
                          {
                            "qualifierCode": "A",
                            "qualifierDescription": "Approved for publication -- Processing and review completed.",
                            "qualifierID": 0,
                            "network": "NWIS",
                            "vocabulary": "uv_rmk_cd"
                          }
                        ],
                        "qualityControlLevel": [],
                        "method": [
                          {
                            "methodDescription": "",
                            "methodID": 42150
                          }
                        ],
                        "source": [],
                        "offset": [],
                        "sample": [],
                        "censorCode": []
                      }
                    ],
                    "name": "USGS:16211600:00060:00000"
                  },
                  {
                    "sourceInfo": {
                      "siteName": "Makaha Str nr Makaha, Oahu, HI",
                      "siteCode": [
                        {
                          "value": "16211600",
                          "network": "NWIS",
                          "agencyCode": "USGS"
                        }
                      ],
                      "timeZoneInfo": {
                        "defaultTimeZone": {
                          "zoneOffset": "-10:00",
                          "zoneAbbreviation": "HST"
                        },
                        "daylightSavingsTimeZone": {
                          "zoneOffset": "-09:00",
                          "zoneAbbreviation": "HDT"
                        },
                        "siteUsesDaylightSavingsTime": false
                      },
                      "geoLocation": {
                        "geogLocation": {
                          "srs": "EPSG:4326",
                          "latitude": 21.50158333,
                          "longitude": -158.1801667
                        },
                        "localSiteXY": []
                      },
                      "note": [],
                      "siteType": [],
                      "siteProperty": [
                        {
                          "value": "ST",
                          "name": "siteTypeCd"
                        },
                        {
                          "value": "20060000",
                          "name": "hucCd"
                        },
                        {
                          "value": "15",
                          "name": "stateCd"
                        },
                        {
                          "value": "15003",
                          "name": "countyCd"
                        }
                      ]
                    },
                    "variable": {
                      "variableCode": [
                        {
                          "value": "00065",
                          "network": "NWIS",
                          "vocabulary": "NWIS:UnitValues",
                          "variableID": 45807202,
                          "default": true
                        }
                      ],
                      "variableName": "Gage height, ft",
                      "variableDescription": "Gage height, feet",
                      "valueType": "Derived Value",
                      "unit": {
                        "unitCode": "ft"
                      },
                      "options": {
                        "option": [
                          {
                            "name": "Statistic",
                            "optionCode": "00000"
                          }
                        ]
                      },
                      "note": [],
                      "noDataValue": -999999,
                      "variableProperty": [],
                      "oid": "45807202"
                    },
                    "values": [
                      {
                        "value": [
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:05:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:10:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:15:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:20:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:25:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:30:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:35:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:40:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:45:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:50:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T14:55:00.000-10:00"
                          },
                          {
                            "value": "0.65",
                            "qualifiers": [
                              "A"
                            ],
                            "dateTime": "2022-12-31T15:00:00.000-10:00"
                          }
                        ],
                        "qualifier": [
                          {
                            "qualifierCode": "A",
                            "qualifierDescription": "Approved for publication -- Processing and review completed.",
                            "qualifierID": 0,
                            "network": "NWIS",
                            "vocabulary": "uv_rmk_cd"
                          }
                        ],
                        "qualityControlLevel": [],
                        "method": [
                          {
                            "methodDescription": "",
                            "methodID": 42151
                          }
                        ],
                        "source": [],
                        "offset": [],
                        "sample": [],
                        "censorCode": []
                      }
                    ],
                    "name": "USGS:16211600:00065:00000"
                  }
                ]
              },
              "nil": false,
              "globalScope": true,
              "typeSubstituted": false
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
    void testReadReturnsOneTimeSeries()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WATERML,
                                               fakeDeclarationSource,
                                               dataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.LEFT,
                                               null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        NwisReader reader = NwisReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Feature featureKey = Feature.of( MessageUtilities.getGeometry( "09165000",
                                                                           "DOLORES RIVER BELOW RICO, CO.",
                                                                           4326,
                                                                           "POINT ( -108.0603517 37.63888428 )" ) );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                                 TimeScaleOuter.of(),
                                                                 "00060",
                                                                 featureKey,
                                                                 "ft3/s" );
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2018-10-01T00:00:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T00:15:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T00:30:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T00:45:00Z" ),
                                                                         11.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2018-10-01T01:00:00Z" ),
                                                                         11.5 ) )
                                                    .setMetadata( metadata )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadReturnsThreeChunkedTimeSeriesRequests()
    {
        // Create the chunk parameters
        Parameters parametersOne = new Parameters( new Parameter( "indent", "on" ),
                                                   new Parameter( "endDT", "2019-01-01T00:00:00Z" ),
                                                   new Parameter( "format", "json" ),
                                                   new Parameter( "parameterCd", "00060" ),
                                                   new Parameter( "sites", "09165000" ),
                                                   new Parameter( "startDT", "2018-01-01T00:00:01Z" ) );

        Parameters parametersTwo = new Parameters( new Parameter( "indent", "on" ),
                                                   new Parameter( "endDT", "2020-01-01T00:00:00Z" ),
                                                   new Parameter( "format", "json" ),
                                                   new Parameter( "parameterCd", "00060" ),
                                                   new Parameter( "sites", "09165000" ),
                                                   new Parameter( "startDT", "2019-01-01T00:00:01Z" ) );

        Parameters parametersThree = new Parameters( new Parameter( "indent", "on" ),
                                                     new Parameter( "endDT", "2021-01-01T00:00:00Z" ),
                                                     new Parameter( "format", "json" ),
                                                     new Parameter( "parameterCd", "00060" ),
                                                     new Parameter( "sites", "09165000" ),
                                                     new Parameter( "startDT", "2020-01-01T00:00:01Z" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "00060" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WATERML,
                                               fakeDeclarationSource,
                                               dataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.LEFT,
                                               null );

        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( "2018-01-01T00:00:00Z" ) )
                                                     .maximum( Instant.parse( "2021-01-01T00:00:00Z" ) )
                                                     .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "09165000" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .leadTimes( leadTimes )
                                                                        .validDates( validDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .features( features )
                                                                        .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        NwisReader reader = NwisReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // Three requests made
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH ),
                                VerificationTimes.exactly( 3 ) );

        // One request made with parameters one
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters two
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters three
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree ),
                                VerificationTimes.exactly( 1 ) );

    }

    @Test
    void testReadReturnsThreeChunkedTimeSeriesRequestsForGeneratedBaselineWithTimeIntervalSet()
    {
        // Create the chunk parameters
        Parameters parametersOne = new Parameters( new Parameter( "indent", "on" ),
                                                   new Parameter( "endDT", "2019-01-01T00:00:00Z" ),
                                                   new Parameter( "format", "json" ),
                                                   new Parameter( "parameterCd", "00060" ),
                                                   new Parameter( "sites", "09165000" ),
                                                   new Parameter( "startDT", "2018-01-01T00:00:01Z" ) );

        Parameters parametersTwo = new Parameters( new Parameter( "indent", "on" ),
                                                   new Parameter( "endDT", "2020-01-01T00:00:00Z" ),
                                                   new Parameter( "format", "json" ),
                                                   new Parameter( "parameterCd", "00060" ),
                                                   new Parameter( "sites", "09165000" ),
                                                   new Parameter( "startDT", "2019-01-01T00:00:01Z" ) );

        Parameters parametersThree = new Parameters( new Parameter( "indent", "on" ),
                                                     new Parameter( "endDT", "2021-01-01T00:00:00Z" ),
                                                     new Parameter( "format", "json" ),
                                                     new Parameter( "parameterCd", "00060" ),
                                                     new Parameter( "sites", "09165000" ),
                                                     new Parameter( "startDT", "2020-01-01T00:00:01Z" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "00060" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WATERML,
                                               fakeDeclarationSource,
                                               dataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.BASELINE,
                                               null );

        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        Instant earliest = Instant.parse( "2018-01-01T00:00:00Z" );
        Instant later = Instant.parse( "2019-01-01T00:00:00Z" );
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( earliest )
                                                     .maximum( later )
                                                     .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setBaseline( Geometry.newBuilder()
                                                                                   .setName( "09165000" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        Instant latest = Instant.parse( "2021-01-01T00:00:00Z" );
        GeneratedBaseline generatedBaseline = GeneratedBaselineBuilder.builder()
                                                                      .method( GeneratedBaselines.CLIMATOLOGY )
                                                                      .minimumDate( earliest )
                                                                      .maximumDate( latest )
                                                                      .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( dataset )
                                                         .generatedBaseline( generatedBaseline )
                                                         .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .baseline( baseline )
                                                                        .leadTimes( leadTimes )
                                                                        .validDates( validDates )
                                                                        .referenceDatePools( referenceTimePools )
                                                                        .features( features )
                                                                        .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        NwisReader reader = NwisReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // Three requests made
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH ),
                                VerificationTimes.exactly( 3 ) );

        // One request made with parameters one
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersOne ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters two
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersTwo ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters three
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( PATH )
                                         .withQueryStringParameters( parametersThree ),
                                VerificationTimes.exactly( 1 ) );

    }

    @Test
    void testReadReturnsOneTimeSeriesForEachOfTwoVariables()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( MULTIVARIATE_RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + PATH
                                  + MULTIVARIATE_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "00060" )
                                                                  .aliases( Set.of( "00065" ) )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WATERML,
                                               fakeDeclarationSource,
                                               dataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.LEFT,
                                               null );

        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( "2023-01-01T00:00:00Z" ) )
                                                     .maximum( Instant.parse( "2023-01-01T01:00:00Z" ) )
                                                     .build();
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder()
                                                                               .setName( "16211600" ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( validDates )
                                                                        .features( features )
                                                                        .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        NwisReader reader = NwisReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Feature featureKey = Feature.of( MessageUtilities.getGeometry( "16211600",
                                                                           "Makaha Str nr Makaha, Oahu, HI",
                                                                           4326,
                                                                           "POINT ( -158.1801667 21.50158333 )" ) );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                                 TimeScaleOuter.of(),
                                                                 "00060",
                                                                 featureKey,
                                                                 "ft3/s" );
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2023-01-01T00:05:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:10:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:15:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:20:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:25:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:30:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:35:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:40:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:45:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:50:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:55:00Z" ),
                                                                         0.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T01:00:00Z" ),
                                                                         0.0 ) )
                                                    .setMetadata( metadata )
                                                    .build();

            TimeSeriesMetadata metadataTwo = TimeSeriesMetadata.of( Map.of(),
                                                                    TimeScaleOuter.of(),
                                                                    "00065",
                                                                    featureKey,
                                                                    "ft" );
            TimeSeries<Double> expectedSeriesTwo =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2023-01-01T00:05:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:10:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:15:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:20:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:25:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:30:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:35:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:40:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:45:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:50:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T00:55:00Z" ),
                                                                         0.65 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-01-01T01:00:00Z" ),
                                                                         0.65 ) )
                                                    .setMetadata( metadataTwo )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries, expectedSeriesTwo );

            assertEquals( expected, actual );
        }
    }
}
