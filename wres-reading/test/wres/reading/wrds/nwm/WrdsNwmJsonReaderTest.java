package wres.reading.wrds.nwm;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.types.Ensemble;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link WrdsNwmJsonReader}.
 * @author James Brown
 */

class WrdsNwmJsonReaderTest
{
    private DataSource fakeSource;

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

        this.fakeSource = DataSource.builder()
                                    .disposition( DataSource.DataDisposition.JSON_WRDS_NWM )
                                    .source( fakeDeclarationSource )
                                    .context( dataset )
                                    .links( Collections.emptyList() )
                                    .uri( fakeUri )
                                    .datasetOrientation( DatasetOrientation.LEFT )
                                    .build();
    }

    @Test
    void testReadTwoMediumRangeEnsembleForecastTimeSeries() throws IOException
    {
        // First trace is deterministic, which is longer than the others and should be snipped to produce a "valid"
        // ensemble. See GitHub #815.
        String jsonString = """
                [
                  {
                    "nwm_feature_id": 5907079,
                    "configuration": "medium_range",
                    "reference_datetime": "2026-07-08T12:00:00Z",
                    "data_type": "streamflow",
                    "units": "CMS",
                    "forecast": [
                      {
                        "member_id": 1,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T13:00:00Z",
                            "value": 1.0
                          },
                          {
                            "valid_datetime": "2026-07-08T14:00:00Z",
                            "value": 2.0
                          },
                          {
                            "valid_datetime": "2026-07-08T15:00:00Z",
                            "value": 3.0
                          }
                        ]
                      },
                      {
                        "member_id": 2,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T13:00:00Z",
                            "value": 3.0
                          },
                          {
                            "valid_datetime": "2026-07-08T14:00:00Z",
                            "value": 4.0
                          }
                        ]
                      }
                    ]
                  },
                  {
                    "nwm_feature_id": 5907079,
                    "configuration": "medium_range",
                    "reference_datetime": "2026-07-08T18:00:00Z",
                    "data_type": "streamflow",
                    "units": "CMS",
                    "forecast": [
                      {
                        "member_id": 1,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T19:00:00Z",
                            "value": 5.0
                          },
                          {
                            "valid_datetime": "2026-07-08T20:00:00Z",
                            "value": 6.0
                          },
                          {
                            "valid_datetime": "2026-07-08T21:00:00Z",
                            "value": 7.0
                          }
                        ]
                      },
                      {
                        "member_id": 2,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T19:00:00Z",
                            "value": 7.0
                          },
                          {
                            "valid_datetime": "2026-07-08T20:00:00Z",
                            "value": 8.0
                          }
                        ]
                      }
                    ]
                  }
                ]
                """;

        WrdsNwmJsonReader reader = WrdsNwmJsonReader.of();

        try ( InputStream inputStream = new ByteArrayInputStream( jsonString.getBytes() );
              Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
        {
            List<TimeSeries<Ensemble>> actual = tupleStream.map( TimeSeriesTuple::getEnsembleTimeSeries )
                                                           .toList();

            TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                            Instant.parse( "2026-07-08T12:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    Feature.of( MessageUtilities.getGeometry( "5907079" ) ),
                                                                    "CMS" );
            TimeSeries<Ensemble> expectedSeriesOne =
                    new TimeSeries.Builder<Ensemble>().addEvent( Event.of( Instant.parse( "2026-07-08T13:00:00Z" ),
                                                                           Ensemble.of( new double[] { 1.0, 3.0 },
                                                                                        Ensemble.Labels.of( "1",
                                                                                                            "2" ) ) ) )
                                                      .addEvent( Event.of( Instant.parse( "2026-07-08T14:00:00Z" ),
                                                                           Ensemble.of( new double[] { 2.0, 4.0 },
                                                                                        Ensemble.Labels.of( "1",
                                                                                                            "2" ) ) ) )
                                                      .addEvent( Event.of( Instant.parse( "2026-07-08T15:00:00Z" ),
                                                                           Ensemble.of( new double[] { 3.0 },
                                                                                        Ensemble.Labels.of( "1" ) ) ) )
                                                      .setMetadata( metadataOne )
                                                      .build();

            TimeSeriesMetadata metadataTwo = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                            Instant.parse( "2026-07-08T18:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    Feature.of( MessageUtilities.getGeometry( "5907079" ) ),
                                                                    "CMS" );
            TimeSeries<Ensemble> expectedSeriesTwo =
                    new TimeSeries.Builder<Ensemble>().addEvent( Event.of( Instant.parse( "2026-07-08T19:00:00Z" ),
                                                                           Ensemble.of( new double[] { 5.0, 7.0 },
                                                                                        Ensemble.Labels.of( "1",
                                                                                                            "2" ) ) ) )
                                                      .addEvent( Event.of( Instant.parse( "2026-07-08T20:00:00Z" ),
                                                                           Ensemble.of( new double[] { 6.0, 8.0 },
                                                                                        Ensemble.Labels.of( "1",
                                                                                                            "2" ) ) ) )
                                                      .addEvent( Event.of( Instant.parse( "2026-07-08T21:00:00Z" ),
                                                                           Ensemble.of( new double[] { 7.0 },
                                                                                        Ensemble.Labels.of( "1" ) ) ) )
                                                      .setMetadata( metadataTwo )
                                                      .build();

            List<TimeSeries<Ensemble>> expected = List.of( expectedSeriesOne, expectedSeriesTwo );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadTwoAnalysisAndAssimilationTimeSeries() throws IOException
    {
        String jsonString = """
                [
                  {
                    "nwm_feature_id": 5907079,
                    "configuration": "analysis_assim",
                    "reference_datetime": "2026-07-08T23:00:00Z",
                    "data_type": "streamflow",
                    "units": "CMS",
                    "forecast": [
                      {
                        "member_id": 1,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T23:00:00Z",
                            "value": 21.12
                          },
                          {
                            "valid_datetime": "2026-07-08T22:00:00Z",
                            "value": 21.8
                          }
                        ]
                      }
                    ]
                  },
                  {
                    "nwm_feature_id": 5907079,
                    "configuration": "analysis_assim",
                    "reference_datetime": "2026-07-08T22:00:00Z",
                    "data_type": "streamflow",
                    "units": "CMS",
                    "forecast": [
                      {
                        "member_id": 1,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T22:00:00Z",
                            "value": 22.17
                          },
                          {
                            "valid_datetime": "2026-07-08T21:00:00Z",
                            "value": 22.14
                          }
                        ]
                      }
                    ]
                  }
                ]
                """;

        WrdsNwmJsonReader reader = WrdsNwmJsonReader.of();

        try ( InputStream inputStream = new ByteArrayInputStream( jsonString.getBytes() );
              Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                                                            Instant.parse( "2026-07-08T23:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    Feature.of( MessageUtilities.getGeometry( "5907079" ) ),
                                                                    "CMS" );
            TimeSeries<Double> expectedSeriesOne =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2026-07-08T23:00:00Z" ),
                                                                         21.12 ) )
                                                    .addEvent( Event.of( Instant.parse( "2026-07-08T22:00:00Z" ),
                                                                         21.8 ) )
                                                    .setMetadata( metadataOne )
                                                    .build();

            TimeSeriesMetadata metadataTwo = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                                                            Instant.parse( "2026-07-08T22:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    Feature.of( MessageUtilities.getGeometry( "5907079" ) ),
                                                                    "CMS" );
            TimeSeries<Double> expectedSeriesTwo =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2026-07-08T22:00:00Z" ),
                                                                         22.17 ) )
                                                    .addEvent( Event.of( Instant.parse( "2026-07-08T21:00:00Z" ),
                                                                         22.14 ) )
                                                    .setMetadata( metadataTwo )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeriesOne, expectedSeriesTwo );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadThreeShortRangeForecastTimeSeries() throws IOException
    {
        String jsonString = """
                [
                  {
                    "nwm_feature_id": 5907079,
                    "configuration": "short_range",
                    "reference_datetime": "2026-07-08T23:00:00Z",
                    "data_type": "streamflow",
                    "units": "CMS",
                    "forecast": [
                      {
                        "member_id": 1,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-09T00:00:00Z",
                            "value": 1.0
                          },
                          {
                            "valid_datetime": "2026-07-09T01:00:00Z",
                            "value": 2.0
                          }
                        ]
                      }
                    ]
                  },
                  {
                    "nwm_feature_id": 5907079,
                    "configuration": "short_range",
                    "reference_datetime": "2026-07-08T22:00:00Z",
                    "data_type": "streamflow",
                    "units": "CMS",
                    "forecast": [
                      {
                        "member_id": 1,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T23:00:00Z",
                            "value": 3.0
                          },
                          {
                            "valid_datetime": "2026-07-09T00:00:00Z",
                            "value": 4.0
                          }
                        ]
                      }
                    ]
                  },
                  {
                    "nwm_feature_id": 5907079,
                    "configuration": "short_range",
                    "reference_datetime": "2026-07-08T21:00:00Z",
                    "data_type": "streamflow",
                    "units": "CMS",
                    "forecast": [
                      {
                        "member_id": 1,
                        "timeseries": [
                          {
                            "valid_datetime": "2026-07-08T22:00:00Z",
                            "value": 5.0
                          },
                          {
                            "valid_datetime": "2026-07-08T23:00:00Z",
                            "value": 6.0
                          }
                        ]
                      }
                    ]
                  }
                ]
                """;

        WrdsNwmJsonReader reader = WrdsNwmJsonReader.of();

        try ( InputStream inputStream = new ByteArrayInputStream( jsonString.getBytes() );
              Stream<TimeSeriesTuple> tupleStream = reader.read( this.fakeSource, inputStream ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                            Instant.parse( "2026-07-08T23:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    Feature.of( MessageUtilities.getGeometry( "5907079" ) ),
                                                                    "CMS" );
            TimeSeries<Double> expectedSeriesOne =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2026-07-09T00:00:00Z" ),
                                                                         1.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2026-07-09T01:00:00Z" ),
                                                                         2.0 ) )
                                                    .setMetadata( metadataOne )
                                                    .build();

            TimeSeriesMetadata metadataTwo = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                            Instant.parse( "2026-07-08T22:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    Feature.of( MessageUtilities.getGeometry( "5907079" ) ),
                                                                    "CMS" );
            TimeSeries<Double> expectedSeriesTwo =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2026-07-08T23:00:00Z" ),
                                                                         3.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2026-07-09T00:00:00Z" ),
                                                                         4.0 ) )
                                                    .setMetadata( metadataTwo )
                                                    .build();

            TimeSeriesMetadata metadataThree = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0,
                                                                            Instant.parse( "2026-07-08T21:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    Feature.of( MessageUtilities.getGeometry( "5907079" ) ),
                                                                    "CMS" );
            TimeSeries<Double> expectedSeriesThree =
                    new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "2026-07-08T22:00:00Z" ),
                                                                         5.0 ) )
                                                    .addEvent( Event.of( Instant.parse( "2026-07-08T23:00:00Z" ),
                                                                         6.0 ) )
                                                    .setMetadata( metadataThree )
                                                    .build();

            List<TimeSeries<Double>> expected = List.of( expectedSeriesOne, expectedSeriesTwo, expectedSeriesThree );

            assertEquals( expected, actual );
        }
    }
}
