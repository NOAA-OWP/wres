package wres.io.reading.wrds;

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

import com.fasterxml.jackson.core.JsonProcessingException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesTuple;
import wres.io.reading.DataSource;
import wres.io.reading.DataSource.DataDisposition;
import wres.statistics.generated.Geometry;

/**
 * Tests the {@link WrdsAhpsJsonReader}.
 * @author James Brown
 */

class WrdsAhpsJsonReaderTest
{
    private DataSource fakeSource;
    private String jsonString;

    @BeforeEach
    void setup() throws JsonProcessingException
    {
        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( null,
                                             null,
                                             null,
                                             null,
                                             null );
        this.fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
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
                                         // Use a fake URI with an NWIS-like string as this is used to trigger the 
                                         // identification of an instantaneous time-scale 
                                         URI.create( "https://fake.wrds.gov/" ),
                                         LeftOrRightOrBaseline.RIGHT );
        this.jsonString =
                "{\n"
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
                                                                 FeatureKey.of( geometry ),
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
}
