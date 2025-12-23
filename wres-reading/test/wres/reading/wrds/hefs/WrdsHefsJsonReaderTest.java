package wres.reading.wrds.hefs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.config.components.SourceInterface;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.types.Ensemble;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime;

/**
 * Tests the {@link WrdsHefsJsonReader}.
 *
 * @author James Brown
 */
class WrdsHefsJsonReaderTest
{
    /** Test document. */
    private static final String TEST_DOCUMENT = """
            [
              [
                {
                  "creation_datetime": "2025-10-05T14:51:00Z",
                  "end_datetime": "2025-11-04T12:00:00Z",
                  "ensemble_id": "MEFP",
                  "ensemble_member_index": 1992,
                  "forecast_datetime": "2025-10-05T12:00:00Z",
                  "lat": 32.02,
                  "location_id": "RDBN5",
                  "lon": -104.05,
                  "parameter_id": "QINE",
                  "start_datetime": "2025-10-05T12:00:00Z",
                  "station_name": "RDBN5 - Red Bluff NM - Delaware River",
                  "time_step_multiplier": "21600",
                  "time_step_unit": "second",
                  "type": "instantaneous",
                  "units": "CFS",
                  "x": -104.05,
                  "y": 32.02,
                  "z": 883.92,
                  "events": [
                    {
                      "flag": "2",
                      "value": 12.2653,
                      "valid_datetime": "2025-10-05T12:00:00Z"
                    },
                    {
                      "flag": "2",
                      "value": 29.3172,
                      "valid_datetime": "2025-10-05T18:00:00Z"
                    }
                  ]
                },
                {
                  "creation_datetime": "2025-10-05T14:51:00Z",
                  "end_datetime": "2025-11-04T12:00:00Z",
                  "ensemble_id": "MEFP",
                  "ensemble_member_index": 1993,
                  "forecast_datetime": "2025-10-05T12:00:00Z",
                  "lat": 32.02,
                  "location_id": "RDBN5",
                  "lon": -104.05,
                  "parameter_id": "QINE",
                  "start_datetime": "2025-10-05T12:00:00Z",
                  "station_name": "RDBN5 - Red Bluff NM - Delaware River",
                  "time_step_multiplier": "21600",
                  "time_step_unit": "second",
                  "type": "instantaneous",
                  "units": "CFS",
                  "x": -104.05,
                  "y": 32.02,
                  "z": 883.92,
                  "events": [
                    {
                      "flag": "2",
                      "value": 18.93,
                      "valid_datetime": "2025-10-05T12:00:00Z"
                    },
                    {
                      "flag": "2",
                      "value": 7.91,
                      "valid_datetime": "2025-10-05T18:00:00Z"
                    }
                  ]
                }
              ]
            ]
            """;

    /** Test document with a missing forecast datetime. */
    private static final String TEST_DOCUMENT_WITHOUT_FORECAST_DATETIME = """
            [
              [
                {
                  "creation_datetime": "2025-10-05T14:51:00Z",
                  "end_datetime": "2025-11-04T12:00:00Z",
                  "ensemble_id": "MEFP",
                  "ensemble_member_index": 1992,
                  "lat": 32.02,
                  "location_id": "RDBN5",
                  "lon": -104.05,
                  "parameter_id": "QINE",
                  "start_datetime": "2025-10-05T12:00:00Z",
                  "station_name": "RDBN5 - Red Bluff NM - Delaware River",
                  "time_step_multiplier": "21600",
                  "time_step_unit": "second",
                  "type": "instantaneous",
                  "units": "CFS",
                  "x": -104.05,
                  "y": 32.02,
                  "z": 883.92,
                  "events": [
                    {
                      "flag": "2",
                      "value": 12.2653,
                      "valid_datetime": "2025-10-05T12:00:00Z"
                    },
                    {
                      "flag": "2",
                      "value": 29.3172,
                      "valid_datetime": "2025-10-05T18:00:00Z"
                    }
                  ]
                },
                {
                  "creation_datetime": "2025-10-05T14:51:00Z",
                  "end_datetime": "2025-11-04T12:00:00Z",
                  "ensemble_id": "MEFP",
                  "ensemble_member_index": 1993,
                  "lat": 32.02,
                  "location_id": "RDBN5",
                  "lon": -104.05,
                  "parameter_id": "QINE",
                  "start_datetime": "2025-10-05T12:00:00Z",
                  "station_name": "RDBN5 - Red Bluff NM - Delaware River",
                  "time_step_multiplier": "21600",
                  "time_step_unit": "second",
                  "type": "instantaneous",
                  "units": "CFS",
                  "x": -104.05,
                  "y": 32.02,
                  "z": 883.92,
                  "events": [
                    {
                      "flag": "2",
                      "value": 18.93,
                      "valid_datetime": "2025-10-05T12:00:00Z"
                    },
                    {
                      "flag": "2",
                      "value": 7.91,
                      "valid_datetime": "2025-10-05T18:00:00Z"
                    }
                  ]
                }
              ]
            ]
            """;

    @Test
    void testRead() throws IOException
    {
        WrdsHefsJsonReader reader = WrdsHefsJsonReader.of();

        try ( InputStream stream = new ByteArrayInputStream( TEST_DOCUMENT.getBytes() ) )
        {
            URI fakeUri = URI.create( "foo.bar/hefs/v1/ensembles/" );
            Source fakeDeclarationSource = SourceBuilder.builder()
                                                        .uri( fakeUri )
                                                        .sourceInterface( SourceInterface.WRDS_HEFS )
                                                        .build();

            Dataset fakeDataset = DatasetBuilder.builder()
                                                .sources( List.of( fakeDeclarationSource ) )
                                                .build();

            DataSource fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_HEFS,
                                                   fakeDeclarationSource,
                                                   fakeDataset,
                                                   Collections.emptyList(),
                                                   fakeUri,
                                                   DatasetOrientation.RIGHT,
                                                   null );

            List<TimeSeries<Ensemble>> actual = reader.read( fakeSource, stream )
                                                      .map( TimeSeriesTuple::getEnsembleTimeSeries )
                                                      .toList();

            Geometry geometry = MessageUtilities.getGeometry( "RDBN5",
                                                              "RDBN5 - Red Bluff NM - Delaware River",
                                                              null,
                                                              "POINT ( -104.05 32.02 883.92 )" );
            TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                    .setVariableName( "QINE" )
                    .setUnit( "CFS" )
                    .setFeature( Feature.of( geometry ) )
                    .setTimeScale( TimeScaleOuter.of() )
                    .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                Instant.parse( "2025-10-05T12:00:00Z" ) ) )
                    .build();

            Instant instant = Instant.parse( "2025-10-05T12:00:00Z" );
            Ensemble ensemble = Ensemble.of( new double[] { 12.2653, 18.93 },
                                             Ensemble.Labels.of( "1992", "1993" ) );
            Event<Ensemble> event = Event.of( instant, ensemble );

            Instant anotherInstant = Instant.parse( "2025-10-05T18:00:00Z" );
            Ensemble anotherEnsemble = Ensemble.of( new double[] { 29.3172, 7.91 },
                                                    Ensemble.Labels.of( "1992", "1993" ) );
            Event<Ensemble> anotherEvent = Event.of( anotherInstant, anotherEnsemble );

            SortedSet<Event<Ensemble>> events = new TreeSet<>();
            events.add( event );
            events.add( anotherEvent );
            List<TimeSeries<Ensemble>> expected = List.of( TimeSeries.of( metadata, events ) );

            assertEquals( expected, actual );
        }
    }

    @Test
    void testReadWithoutForecastDateTime() throws IOException
    {
        // See GitHub #138 - this can occur in the wild

        WrdsHefsJsonReader reader = WrdsHefsJsonReader.of();

        try ( InputStream stream = new ByteArrayInputStream( TEST_DOCUMENT_WITHOUT_FORECAST_DATETIME.getBytes() ) )
        {
            URI fakeUri = URI.create( "foo.bar/hefs/v1/ensembles/" );
            Source fakeDeclarationSource = SourceBuilder.builder()
                                                        .uri( fakeUri )
                                                        .sourceInterface( SourceInterface.WRDS_HEFS )
                                                        .build();

            Dataset fakeDataset = DatasetBuilder.builder()
                                                .sources( List.of( fakeDeclarationSource ) )
                                                .build();

            DataSource fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_HEFS,
                                                   fakeDeclarationSource,
                                                   fakeDataset,
                                                   Collections.emptyList(),
                                                   fakeUri,
                                                   DatasetOrientation.RIGHT,
                                                   null );

            List<TimeSeries<Ensemble>> actual = reader.read( fakeSource, stream )
                                                      .map( TimeSeriesTuple::getEnsembleTimeSeries )
                                                      .toList();

            Geometry geometry = MessageUtilities.getGeometry( "RDBN5",
                                                              "RDBN5 - Red Bluff NM - Delaware River",
                                                              null,
                                                              "POINT ( -104.05 32.02 883.92 )" );
            TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                    .setVariableName( "QINE" )
                    .setUnit( "CFS" )
                    .setFeature( Feature.of( geometry ) )
                    .setReferenceTimes( Map.of() )
                    .setTimeScale( TimeScaleOuter.of() )
                    .build();

            Instant instant = Instant.parse( "2025-10-05T12:00:00Z" );
            Ensemble ensemble = Ensemble.of( new double[] { 12.2653, 18.93 },
                                             Ensemble.Labels.of( "1992", "1993" ) );
            Event<Ensemble> event = Event.of( instant, ensemble );

            Instant anotherInstant = Instant.parse( "2025-10-05T18:00:00Z" );
            Ensemble anotherEnsemble = Ensemble.of( new double[] { 29.3172, 7.91 },
                                                    Ensemble.Labels.of( "1992", "1993" ) );
            Event<Ensemble> anotherEvent = Event.of( anotherInstant, anotherEnsemble );

            SortedSet<Event<Ensemble>> events = new TreeSet<>();
            events.add( event );
            events.add( anotherEvent );
            List<TimeSeries<Ensemble>> expected = List.of( TimeSeries.of( metadata, events ) );

            assertEquals( expected, actual );
        }
    }
}
