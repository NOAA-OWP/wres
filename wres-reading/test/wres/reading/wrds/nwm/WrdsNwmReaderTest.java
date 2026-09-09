package wres.reading.wrds.nwm;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.components.DataType;
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
import wres.config.components.UriParameter;
import wres.config.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.system.SystemSettings;

/**
 * Tests the {@link WrdsNwmReader}.
 * @author James Brown
 */

class WrdsNwmReaderTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReaderTest.class );

    @RegisterExtension
    private static final WireMockExtension WIREMOCK = WireMockExtension.newInstance()
                                                                       .options( WireMockConfiguration.wireMockConfig()
                                                                                                      .dynamicPort()
                                                                                                      .dynamicHttpsPort() )
                                                                       .build();

    /** Feature considered. */
    private static final String NWM_FEATURE_ID = "5907079";

    /** Path used by GET for analysis. */
    private static final String ANALYSIS_PATH = "/nwm/v1/streamflow/analysis_assim/nwm_feature_id/"
                                                + NWM_FEATURE_ID
                                                + "/";

    /** Parameters added to path. */
    private static final String ANALYSIS_PARAMS = "?min_reference_datetime=2026-07-08T22:00:00Z"
                                                  + "&max_reference_datetime=2026-07-08T23:00:00Z";

    /** Analysis response from GET. */
    private static final String ANALYSIS_RESPONSE = """
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

    /** Path used by GET for forecasts. */
    private static final String FORECAST_PATH = "/nwm/v1/streamflow/forecast/nwm_feature_id/"
                                                + NWM_FEATURE_ID
                                                + "/";

    /** Forecast response from GET. */
    private static final String FORECAST_RESPONSE = """
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

    @Test
    void testReadReturnsOneAnalysisTimeSeries()
    {
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( ANALYSIS_PATH ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withBody( ANALYSIS_RESPONSE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + ANALYSIS_PATH
                                  + ANALYSIS_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataSource.DataDisposition.JSON_WRDS_NWM )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsNwmReader reader = WrdsNwmReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            Feature feature = Feature.of( MessageUtilities.getGeometry( "5907079" ) );

            TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                                                            Instant.parse( "2026-07-08T23:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    feature,
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
                                                                    feature,
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
    void testReadReturnsOneAnalysisTimeSeriesAfterTwoDroppedConnections()
    {
        LOGGER.debug( "This unit test produces expected exceptions from dropped connections. Ignore any "
                      + "java.io.EOFException or similar that originates from these unit tests during error recovery, "
                      + "prior to success. Two of these exceptions can be expected because a connection is "
                      + "intentionally dropped twice before recovery." );

        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( ANALYSIS_PATH ) )
                                  .inScenario( "Retry Logic" )
                                  .whenScenarioStateIs( Scenario.STARTED ) // Initial state
                                  .willSetStateTo( "First Failure" )
                                  .willReturn( WireMock.aResponse()
                                                       .withFault( Fault.CONNECTION_RESET_BY_PEER ) ) );

        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( ANALYSIS_PATH ) )
                                  .inScenario( "Retry Logic" )
                                  .whenScenarioStateIs( "First Failure" ) // Initial state
                                  .willSetStateTo( "Success State" )
                                  .willReturn( WireMock.aResponse()
                                                       .withFault( Fault.CONNECTION_RESET_BY_PEER ) ) );

        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( ANALYSIS_PATH ) )
                                  .inScenario( "Retry Logic" )
                                  .whenScenarioStateIs( "Success State" )
                                  .willReturn( WireMock.okJson( ANALYSIS_RESPONSE ) ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + ANALYSIS_PATH
                                  + ANALYSIS_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataSource.DataDisposition.JSON_WRDS_NWM )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.LEFT )
                                          .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsNwmReader reader = WrdsNwmReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            Feature feature = Feature.of( MessageUtilities.getGeometry( "5907079" ) );

            TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                                                            Instant.parse( "2026-07-08T23:00:00Z" ) ),
                                                                    TimeScaleOuter.of(),
                                                                    "streamflow",
                                                                    feature,
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
                                                                    feature,
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
    void testReadReturnsThreeChunkedForecastTimeSeries()
    {
        // Create the chunk parameters. Note the fiddly interval notation on the date ranges
        // First chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                  .withQueryParam( "min_reference_datetime",
                                                   WireMock.equalTo( "2022-01-02T00:00:00Z" ) )
                                  .withQueryParam( "max_reference_datetime",
                                                   WireMock.equalTo( "2022-01-08T23:59:59Z" ) )
                                  .withQueryParam( "configuration", WireMock.equalTo( "short_range" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withBody( FORECAST_RESPONSE ) ) );

        // Second chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                  .withQueryParam( "min_reference_datetime",
                                                   WireMock.equalTo( "2022-01-09T00:00:00Z" ) )
                                  .withQueryParam( "max_reference_datetime",
                                                   WireMock.equalTo( "2022-01-15T23:59:59Z" ) )
                                  .withQueryParam( "configuration", WireMock.equalTo( "short_range" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withBody( FORECAST_RESPONSE ) ) );

        // Third chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                  .withQueryParam( "min_reference_datetime",
                                                   WireMock.equalTo( "2022-01-16T00:00:00Z" ) )
                                  .withQueryParam( "max_reference_datetime",
                                                   WireMock.equalTo( "2022-01-23T00:00:00Z" ) )
                                  .withQueryParam( "configuration", WireMock.equalTo( "short_range" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withBody( FORECAST_RESPONSE ) ) );

        // Need to use a short URL, as would be declared, since chunking goes through URL creation
        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + "/nwm/v1/" );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .parameters( List.of( new UriParameter( "configuration",
                                                                                            "short_range" ) ) )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .type( DataType.SINGLE_VALUED_FORECASTS )
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "streamflow" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataSource.DataDisposition.JSON_WRDS_NWM )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.RIGHT )
                                          .build();

        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( "2022-01-03T00:00:00Z" ) )
                                                         .maximum( Instant.parse( "2022-01-23T00:00:00Z" ) )
                                                         .build();

        Set<GeometryTuple> geometries
                = Set.of( GeometryTuple.newBuilder()
                                       .setRight( Geometry.newBuilder()
                                                          .setName( NWM_FEATURE_ID ) )
                                       .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDates( referenceDates )
                                                                        .features( features )
                                                                        .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsNwmReader reader = WrdsNwmReader.of( declaration, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            // Three chunks of three forecasts expected = 9 forecasts
            assertEquals( 9, actual.size() );
        }

        // Three requests made
        WIREMOCK.verify( WireMock.exactly( 3 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                 .withQueryParam( "min_reference_datetime",
                                                  WireMock.equalTo( "2022-01-02T00:00:00Z" ) )
                                 .withQueryParam( "max_reference_datetime",
                                                  WireMock.equalTo( "2022-01-08T23:59:59Z" ) )
                                 .withQueryParam( "configuration", WireMock.equalTo( "short_range" ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                 .withQueryParam( "min_reference_datetime",
                                                  WireMock.equalTo( "2022-01-09T00:00:00Z" ) )
                                 .withQueryParam( "max_reference_datetime",
                                                  WireMock.equalTo( "2022-01-15T23:59:59Z" ) )
                                 .withQueryParam( "configuration", WireMock.equalTo( "short_range" ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                 .withQueryParam( "min_reference_datetime",
                                                  WireMock.equalTo( "2022-01-16T00:00:00Z" ) )
                                 .withQueryParam( "max_reference_datetime",
                                                  WireMock.equalTo( "2022-01-23T00:00:00Z" ) )
                                 .withQueryParam( "configuration", WireMock.equalTo( "short_range" ) ) );
    }

    /**
     * Tests for an expected exception and not an unexpected one. See #109238.
     */

    @Test
    void testReadDoesNotThrowClassCastExceptionWhenChunkingFeatures()
    {
        URI fakeUri = URI.create( "fake" );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .parameters( List.of( new UriParameter( "configuration",
                                                                                            "short_range" ) ) )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .type( DataType.SINGLE_VALUED_FORECASTS )
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "streamflow" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataSource.DataDisposition.JSON_WRDS_NWM )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.RIGHT )
                                          .build();

        TimeInterval referenceDates = TimeIntervalBuilder.builder()
                                                         .minimum( Instant.parse( "2022-01-03T00:00:00Z" ) )
                                                         .maximum( Instant.parse( "2022-01-23T00:00:00Z" ) )
                                                         .build();

        Set<GeometryTuple> geometries
                = Set.of( GeometryTuple.newBuilder()
                                       .setRight( Geometry.newBuilder()
                                                          .setName( NWM_FEATURE_ID ) )
                                       .build(),
                          GeometryTuple.newBuilder()
                                       .setRight( Geometry.newBuilder()
                                                          .setName( "234442421" ) )
                                       .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .referenceDates( referenceDates )
                                                                        .features( features )
                                                                        .build();

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.getPoolObjectLifespan() )
               .thenReturn( 30_000 );

        // Feature chunk size of 1, with 2 features requested
        WrdsNwmReader reader = WrdsNwmReader.of( declaration, systemSettings, 1 );

        // Expect a ReadException due to the fake uri, not a ClassCastException as in #109238.
        assertThrows( ReadException.class, () -> {  // NOSONAR
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
            {
                long count = tupleStream.count();
                LOGGER.debug( "Count: {}.", count );
            }
        } );
    }

    @Test
    void testReadDoesNotThrowNullPointerExceptionWhenChunkingFeaturesWithMissingReferenceDatesForForecastType()
    {
        URI fakeUri = URI.create( "fake" );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .parameters( List.of( new UriParameter( "configuration",
                                                                                            "short_range" ) ) )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .type( DataType.SINGLE_VALUED_FORECASTS )
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "streamflow" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.builder()
                                          .disposition( DataSource.DataDisposition.JSON_WRDS_NWM )
                                          .source( fakeDeclarationSource )
                                          .context( dataset )
                                          .links( Collections.emptyList() )
                                          .uri( fakeUri )
                                          .datasetOrientation( DatasetOrientation.RIGHT )
                                          .build();

        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( "2022-01-03T00:00:00Z" ) )
                                                     .maximum( Instant.parse( "2022-01-23T00:00:00Z" ) )
                                                     .build();

        Set<GeometryTuple> geometries
                = Set.of( GeometryTuple.newBuilder()
                                       .setRight( Geometry.newBuilder()
                                                          .setName( NWM_FEATURE_ID ) )
                                       .build(),
                          GeometryTuple.newBuilder()
                                       .setRight( Geometry.newBuilder()
                                                          .setName( "234442421" ) )
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

        // Feature chunk size of 1, with 2 features requested
        WrdsNwmReader reader = WrdsNwmReader.of( declaration, systemSettings, 1 );

        // Expect a ReadException, not an NPE: GitHub #497
        ReadException exception = assertThrows( ReadException.class, () -> {  // NOSONAR
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
            {
                long count = tupleStream.count();
                LOGGER.debug( "Count: {}.", count );
            }
        } );

        assertTrue( exception.getMessage()
                             .contains( "Encountered a WRDS NWM forecast data source" ) );
    }
}