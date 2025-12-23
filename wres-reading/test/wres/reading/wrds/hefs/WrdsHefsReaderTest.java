package wres.reading.wrds.hefs;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.Features;
import wres.config.components.FeaturesBuilder;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.config.components.SourceInterface;
import wres.config.components.TimeInterval;
import wres.config.components.TimeIntervalBuilder;
import wres.config.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.types.Ensemble;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime;
import wres.system.SystemSettings;

/**
 * Tests the {@link WrdsHefsReader}.
 *
 * @author James Brown
 */

class WrdsHefsReaderTest
{
    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Feature considered. */
    private static final String FEATURE_NAME = "RDBN5";

    /** Path used by GET for forecasts. */
    private static final String FORECAST_PATH = "/hefs/v1/ensembles/";

    /** Forecast response from GET. */
    private static final String FORECAST_RESPONSE = """
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
                                         .withPath( FORECAST_PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( FORECAST_RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + FORECAST_PATH );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_HEFS )
                                                    .build();

        Dataset fakeDataset = DatasetBuilder.builder()
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_HEFS,
                                               fakeDeclarationSource,
                                               fakeDataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.RIGHT,
                                               null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsHefsReader reader = WrdsHefsReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Ensemble>> actual = tupleStream.map( TimeSeriesTuple::getEnsembleTimeSeries )
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
    void testReadProducesHttp404ErrorAndNoNullPointerException()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( FORECAST_PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.notFoundResponse() );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + FORECAST_PATH );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_HEFS )
                                                    .build();

        Dataset fakeDataset = DatasetBuilder.builder()
                                            .sources( List.of( fakeDeclarationSource ) )
                                            .variable( VariableBuilder.builder()
                                                                      .name( "QINE" )
                                                                      .build() )
                                            .build();

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_HEFS,
                                               fakeDeclarationSource,
                                               fakeDataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.RIGHT,
                                               null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

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
        Features features = FeaturesBuilder.builder()
                                           .geometries( Set.of( geometryTuple ) )
                                           .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .validDates( interval )
                                            .features( features )
                                            .build();

        WrdsHefsReader reader = WrdsHefsReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            Assertions.assertDoesNotThrow( () -> tupleStream.map( TimeSeriesTuple::getEnsembleTimeSeries )
                                                            .toList() );
        }
    }
}
