package wres.reading.wrds.ahps;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.reading.DataSource.DataDisposition;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link WrdsAhpsJsonReader}.
 * @author James Brown
 */

class WrdsAhpsJsonReaderTest
{
    private DataSource fakeSource;
    private String jsonString;
    private String observedJsonString;

    @BeforeEach
    void setup()
    {
        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        this.fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                         fakeDeclarationSource,
                                         dataset,
                                         Collections.emptyList(),
                                         // Use a fake URI with an NWIS-like string as this is used to trigger the
                                         // identification of an instantaneous time-scale
                                         URI.create( "https://fake.wrds.gov/" ),
                                         DatasetOrientation.RIGHT,
                                         null );
        this.jsonString =
                """
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

        this.observedJsonString =
                """
                        {
                            "_documentation": {
                                "swaggerURL": "http://WRDS/docs/observed/v1.0/swagger/"
                            },
                            "_deployment": {
                                "apiUrl": "https://WRDS/api/observed/v1.0/observed/streamflow/nws_lid/ERBA2/?proj=HANK",
                                "stack": "prod",
                                "version": "v1.1.1",
                                "apiCaller": "HANK"
                            },
                            "_nonUrgentIssueReportingLink": "omitted",
                            "_metrics": {
                                "observedDataCount": 12,
                                "locationCount": 1,
                                "totalRequestTime": 0.7744016647338867
                            },
                            "header": {
                                "request": {
                                    "params": {
                                        "asProvided": {},
                                        "asUsed": {
                                            "validTime": null,
                                            "nonUSGSGages": false,
                                            "issuer": null,
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
                                            "nwsLid": "ERBA2",
                                            "usgsSiteCode": "",
                                            "nwmFeatureId": "",
                                            "nwsName": "",
                                            "usgsName": ""
                                        },
                                        "nwsCoordinates": {
                                            "latitude": null,
                                            "longitude": null,
                                            "crs": null,
                                            "link": null
                                        },
                                        "usgsCoordinates": {
                                            "latitude": null,
                                            "longitude": null,
                                            "crs": null,
                                            "link": null
                                        }
                                    },
                                    "producer": "",
                                    "issuer": "ACR",
                                    "distributor": "SBN",
                                    "generationTime": "2023-12-01T00:09:28Z",
                                    "parameterCodes": {
                                        "physicalElement": "QR",
                                        "duration": "I",
                                        "typeSource": "RZ"
                                    },
                                    "thresholds": {
                                        "units": "CFS",
                                        "action": null,
                                        "minor": null,
                                        "moderate": null,
                                        "major": null,
                                        "record": 10300.0
                                    },
                                    "timeseries": [
                                        {
                                            "identifier": "1",
                                            "units": "KCFS",
                                            "dataPoints": [
                                                {
                                                    "time": "2023-12-03T00:00:00Z",
                                                    "value": -999.0,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-03T06:00:00Z",
                                                    "value": -999.0,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-03T12:00:00Z",
                                                    "value": -999.0,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-03T18:00:00Z",
                                                    "value": 0.0665,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-04T00:00:00Z",
                                                    "value": 0.185,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-04T12:00:00Z",
                                                    "value": 0.287,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-04T18:00:00Z",
                                                    "value": 0.319,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-05T00:00:00Z",
                                                    "value": 0.544,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-05T06:00:00Z",
                                                    "value": 0.462,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-05T12:00:00Z",
                                                    "value": 0.421,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-05T18:00:00Z",
                                                    "value": 0.36,
                                                    "status": "N/A"
                                                },
                                                {
                                                    "time": "2023-12-06T00:00:00Z",
                                                    "value": 0.342,
                                                    "status": "N/A"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                                        """;
    }

    @Test
    void testReadForecastsFromStreamResultsInOneTimeSeries() throws IOException
    {
        WrdsAhpsJsonReader reader = WrdsAhpsJsonReader.of();

        try ( InputStream inputStream = new ByteArrayInputStream( this.jsonString.getBytes() );
              Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Geometry geometry = MessageFactory.getGeometry( "FROV2",
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
    void testReadObservationsFromStreamResultsInOneTimeSeries() throws IOException
    {
        WrdsAhpsJsonReader reader = WrdsAhpsJsonReader.of();

        try ( InputStream inputStream = new ByteArrayInputStream( this.observedJsonString.getBytes() );
              Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Geometry geometry = MessageFactory.getGeometry( "ERBA2",
                                                            null,
                                                            null,
                                                            null );

            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                                 TimeScaleOuter.of(),
                                                                 "QR",
                                                                 Feature.of( geometry ),
                                                                 "KCFS" );
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2023-12-03T00:00:00Z" ),
                                                                         Double.NaN ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-03T06:00:00Z" ),
                                                                         Double.NaN ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-03T12:00:00Z" ),
                                                                         Double.NaN ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-03T18:00:00Z" ),
                                                                         0.0665 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-04T00:00:00Z" ),
                                                                         0.185 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-04T12:00:00Z" ),
                                                                         0.287 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-04T18:00:00Z" ),
                                                                         0.319 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-05T00:00:00Z" ),
                                                                         0.544 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-05T06:00:00Z" ),
                                                                         0.462 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-05T12:00:00Z" ),
                                                                         0.421 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-05T18:00:00Z" ),
                                                                         0.36 ) )
                                                    .addEvent( Event.of( Instant.parse( "2023-12-06T00:00:00Z" ),
                                                                         0.342 ) )
                                                    .setMetadata( metadata )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }
}

