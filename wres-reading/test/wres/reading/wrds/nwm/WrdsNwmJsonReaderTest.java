package wres.reading.wrds.nwm;

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
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageFactory;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link WrdsNwmJsonReader}.
 * @author James Brown
 */

class WrdsNwmJsonReaderTest
{
    private DataSource fakeSource;
    private String jsonString;

    @BeforeEach
    void setup()
    {
        // Use a fake URI with an NWIS-like string as this is used to trigger the
        // identification of an instantaneous timescale
        URI fakeUri = URI.create( "https://fake.wrds.gov/" );
        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        this.fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_NWM,
                                         fakeDeclarationSource,
                                         dataset,
                                         Collections.emptyList(),
                                         fakeUri,
                                         DatasetOrientation.LEFT );

        this.jsonString =
                """
                        {
                          "_documentation": "https://somewhere/docs/v1/nwm/swagger/",
                          "_metrics": {
                            "location_api_call": 0.08287358283996582,
                            "forming_location_data": 0.0002918243408203125,
                            "usgs_feature_id_count": 1,
                            "other_feature_id_count": 0,
                            "validate_thredds_vars": 1.2268075942993164,
                            "thredds_call": 0.1791837215423584,
                            "thredds_data_forming": 0.0000045299530029296875,
                            "response_forming": 0.000018358230590820312,
                            "total_request_time": 1.85443115234375
                          },
                          "_warnings": [],
                          "variable": {
                            "name": "streamflow",
                            "unit": "meter^3 / sec"
                          },
                          "forecasts": [
                            {
                              "reference_time": "20200112T00Z",
                              "features": [
                                {
                                  "location": {
                                    "names": {
                                      "nws_lid": "",
                                      "usgs_site_code": "07049000",
                                      "nwm_feature_id": "8588002",
                                      "name": "War Eagle Creek near Hindsville  AR"
                                    },
                                    "coordinates": {
                                      "latitude": "36.2",
                                      "longitude": "-93.855"
                                    }
                                  },
                                  "members": [
                                    {
                                      "identifier": "1",
                                      "data_points": [
                                        {
                                          "time": "20200112T03Z",
                                          "value": "270.9899939429015"
                                        },
                                        {
                                          "time": "20200112T02Z",
                                          "value": "334.139992531389"
                                        },
                                        {
                                          "time": "20200112T01Z",
                                          "value": "382.27999145537615"
                                        }
                                      ]
                                    }
                                  ]
                                }
                              ]
                            }
                          ]
                        }""";
    }

    @Test
    void testReadObservationsFromStreamResultsInOneTimeSeries() throws IOException
    {
        WrdsNwmJsonReader reader = WrdsNwmJsonReader.of();

        try ( InputStream inputStream = new ByteArrayInputStream( this.jsonString.getBytes() );
              Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                         Instant.parse( "2020-01-12T00:00:00Z" ) ),
                                                                 null,
                                                                 "streamflow",
                                                                 Feature.of( MessageFactory.getGeometry( "8588002" ) ),
                                                                 "meter^3 / sec" );
            TimeSeries<Double> expectedSeries =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2020-01-12T01:00:00Z" ),
                                                                         382.27999145537615 ) )
                                                    .addEvent( Event.of( Instant.parse( "2020-01-12T02:00:00Z" ),
                                                                         334.139992531389 ) )
                                                    .addEvent( Event.of( Instant.parse( "2020-01-12T03:00:00Z" ),
                                                                         270.9899939429015 ) )
                                                    .setMetadata( metadata )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeries );

            assertEquals( expected, actual );
        }
    }
}
