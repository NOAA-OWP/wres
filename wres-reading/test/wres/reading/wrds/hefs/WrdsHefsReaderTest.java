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

import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
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
    private static final String FEATURE_NAME = "PGRC2";

    /** Path used by GET for forecasts. */
    private static final String FORECAST_PATH = "/hefs/v1/ensembles/";

    /** Forecast response from GET. */
    private static final String FORECAST_RESPONSE = """
            [
              {
                "events": [
                  {
                    "date": "2025-04-21",
                    "flag": "0",
                    "time": "00:00:00",
                    "value": 55.797176
                  },
                  {
                    "date": "2025-04-21",
                    "flag": "0",
                    "time": "06:00:00",
                    "value": 55.090878
                  }
                ],
                "tracking_id": "972df95b-c2b7-4e0d-84af-a60e63a50e9d",
                "type": "instantaneous",
                "location_id": "PGRC2",
                "parameter_id": "QINE",
                "ensemble_id": "MEFP",
                "ensemble_member_index": 1982,
                "time_step_unit": "second",
                "time_step_multiplier": "21600",
                "start_date_date": "2025-04-21",
                "start_date_time": "00:00:00",
                "end_date_date": "2025-05-21",
                "end_date_time": "00:00:00",
                "forecast_date_date": "2025-04-21",
                "forecast_date_time": "00:00:00",
                "miss_val": null,
                "station_name": "PGRC2",
                "lat": 37.1438888889,
                "lon": -104.547222222,
                "x": -104.547222222,
                "y": 37.1438888889,
                "units": "CFS",
                "creation_date": "2025-04-21",
                "creation_time": "01:15:24",
                "module_instance_id": null,
                "qualifier_id": null,
                "approved_date": null,
                "long_name": null,
                "z": 1851.3552,
                "source_organisation": null,
                "source_system": null,
                "file_description": null,
                "region": null
              },
              {
                "events": [
                  {
                    "date": "2025-04-21",
                    "flag": "0",
                    "time": "00:00:00",
                    "value": 149.38104
                  },
                  {
                    "date": "2025-04-21",
                    "flag": "0",
                    "time": "06:00:00",
                    "value": 146.90901
                  }
                ],
                "tracking_id": "370f07bf-412d-486c-bbd9-8db504065fd0",
                "type": "instantaneous",
                "location_id": "PGRC2",
                "parameter_id": "QINE",
                "ensemble_id": "MEFP",
                "ensemble_member_index": 1983,
                "time_step_unit": "second",
                "time_step_multiplier": "21600",
                "start_date_date": "2025-04-21",
                "start_date_time": "00:00:00",
                "end_date_date": "2025-05-21",
                "end_date_time": "00:00:00",
                "forecast_date_date": "2025-04-21",
                "forecast_date_time": "00:00:00",
                "miss_val": null,
                "station_name": "PGRC2",
                "lat": 37.1438888889,
                "lon": -104.547222222,
                "x": -104.547222222,
                "y": 37.1438888889,
                "units": "CFS",
                "creation_date": "2025-04-21",
                "creation_time": "01:15:24",
                "module_instance_id": null,
                "qualifier_id": null,
                "approved_date": null,
                "long_name": null,
                "z": 1851.3552,
                "source_organisation": null,
                "source_system": null,
                "file_description": null,
                "region": null
              }
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

            Geometry geometry = MessageUtilities.getGeometry( "PGRC2",
                                                              null,
                                                              null,
                                                              "POINT ( -104.547222222 37.1438888889 1851.3552 )" );
            TimeSeriesMetadata metadata = new TimeSeriesMetadata.Builder()
                    .setVariableName( "QINE" )
                    .setUnit( "CFS" )
                    .setFeature( Feature.of( geometry ) )
                    .setTimeScale( TimeScaleOuter.of() )
                    .setReferenceTimes( Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                Instant.parse( "2025-04-21T00:00:00Z" ) ) )
                    .build();

            Instant instant = Instant.parse( "2025-04-21T00:00:00Z" );
            Ensemble ensemble = Ensemble.of( new double[] { 55.797176, 149.38104 },
                                             Ensemble.Labels.of( "1982", "1983" ) );
            Event<Ensemble> event = Event.of( instant, ensemble );

            Instant anotherInstant = Instant.parse( "2025-04-21T06:00:00Z" );
            Ensemble anotherEnsemble = Ensemble.of( new double[] { 55.090878, 146.90901 },
                                                    Ensemble.Labels.of( "1982", "1983" ) );
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
