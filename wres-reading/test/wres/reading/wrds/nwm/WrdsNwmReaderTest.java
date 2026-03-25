package wres.reading.wrds.nwm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import wres.config.components.VariableBuilder;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.reading.ReadException;
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
    private static final int NWM_FEATURE_ID = 8588002;

    /** Path used by GET for analysis. */
    private static final String ANALYSIS_PATH = "/api/v1/nwm/ops/analysis_assim/streamflow/nwm_feature_id/"
                                                + NWM_FEATURE_ID
                                                + "/";

    /** Parameters added to path. */
    private static final String ANALYSIS_PARAMS = "?reference_time=2020-01-12T00:00:00Z";

    /** Analysis response from GET. */
    private static final String ANALYSIS_RESPONSE = "{\n"
                                                    + "  \"_documentation\": \"https://somewhere/docs/v1/nwm/swagger/\",\n"
                                                    + "  \"_metrics\": {\n"
                                                    + "    \"location_api_call\": 0.08287358283996582,\n"
                                                    + "    \"forming_location_data\": 0.0002918243408203125,\n"
                                                    + "    \"usgs_feature_id_count\": 1,\n"
                                                    + "    \"other_feature_id_count\": 0,\n"
                                                    + "    \"validate_thredds_vars\": 1.2268075942993164,\n"
                                                    + "    \"thredds_call\": 0.1791837215423584,\n"
                                                    + "    \"thredds_data_forming\": 0.0000045299530029296875,\n"
                                                    + "    \"response_forming\": 0.000018358230590820312,\n"
                                                    + "    \"total_request_time\": 1.85443115234375\n"
                                                    + "  },\n"
                                                    + "  \"_warnings\": [],\n"
                                                    + "  \"variable\": {\n"
                                                    + "    \"name\": \"streamflow\",\n"
                                                    + "    \"unit\": \"meter^3 / sec\"\n"
                                                    + "  },\n"
                                                    + "  \"forecasts\": [\n"
                                                    + "    {\n"
                                                    + "      \"reference_time\": \"2020-01-12T00:00:00Z\",\n"
                                                    + "      \"features\": [\n"
                                                    + "        {\n"
                                                    + "          \"location\": {\n"
                                                    + "            \"names\": {\n"
                                                    + "              \"nws_lid\": \"\",\n"
                                                    + "              \"usgs_site_code\": \"07049000\",\n"
                                                    + "              \"nwm_feature_id\": \""
                                                    + NWM_FEATURE_ID
                                                    + "\",\n"
                                                    + "              \"name\": \"War Eagle Creek near Hindsville  AR\"\n"
                                                    + "            },\n"
                                                    + "            \"coordinates\": {\n"
                                                    + "              \"latitude\": \"36.2\",\n"
                                                    + "              \"longitude\": \"-93.855\"\n"
                                                    + "            }\n"
                                                    + "          },\n"
                                                    + "          \"members\": [\n"
                                                    + "            {\n"
                                                    + "              \"identifier\": \"1\",\n"
                                                    + "              \"data_points\": [\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T03:00:00Z\",\n"
                                                    + "                  \"value\": \"270.9899939429015\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T02:00:00Z\",\n"
                                                    + "                  \"value\": \"334.139992531389\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T01:00:00Z\",\n"
                                                    + "                  \"value\": \"382.27999145537615\"\n"
                                                    + "                }\n"
                                                    + "              ]\n"
                                                    + "            }\n"
                                                    + "          ]\n"
                                                    + "        }\n"
                                                    + "      ]\n"
                                                    + "    }\n"
                                                    + "  ]\n"
                                                    + "}\n";

    /** Path used by GET for forecasts. */
    private static final String FORECAST_PATH = "/api/v1/nwm/ops/short_range/streamflow/nwm_feature_id/"
                                                + NWM_FEATURE_ID
                                                + "/";

    /** Forecast response from GET. */
    private static final String FORECAST_RESPONSE = "{\n"
                                                    + "  \"_documentation\": \"https://somewhere/docs/v1/nwm/swagger/\",\n"
                                                    + "  \"_metrics\": {\n"
                                                    + "    \"location_api_call\": 0.08287358283996582,\n"
                                                    + "    \"forming_location_data\": 0.0002918243408203125,\n"
                                                    + "    \"usgs_feature_id_count\": 1,\n"
                                                    + "    \"other_feature_id_count\": 0,\n"
                                                    + "    \"validate_thredds_vars\": 1.2268075942993164,\n"
                                                    + "    \"thredds_call\": 0.1791837215423584,\n"
                                                    + "    \"thredds_data_forming\": 0.0000045299530029296875,\n"
                                                    + "    \"response_forming\": 0.000018358230590820312,\n"
                                                    + "    \"total_request_time\": 1.85443115234375\n"
                                                    + "  },\n"
                                                    + "  \"_warnings\": [],\n"
                                                    + "  \"variable\": {\n"
                                                    + "    \"name\": \"streamflow\",\n"
                                                    + "    \"unit\": \"meter^3 / sec\"\n"
                                                    + "  },\n"
                                                    + "  \"forecasts\": [\n"
                                                    + "    {\n"
                                                    + "      \"reference_time\": \"2020-01-12T00:00:00Z\",\n"
                                                    + "      \"features\": [\n"
                                                    + "        {\n"
                                                    + "          \"location\": {\n"
                                                    + "            \"names\": {\n"
                                                    + "              \"nws_lid\": \"\",\n"
                                                    + "              \"usgs_site_code\": \"07049000\",\n"
                                                    + "              \"nwm_feature_id\": \""
                                                    + NWM_FEATURE_ID
                                                    + "\",\n"
                                                    + "              \"name\": \"War Eagle Creek near Hindsville  AR\"\n"
                                                    + "            },\n"
                                                    + "            \"coordinates\": {\n"
                                                    + "              \"latitude\": \"36.2\",\n"
                                                    + "              \"longitude\": \"-93.855\"\n"
                                                    + "            }\n"
                                                    + "          },\n"
                                                    + "          \"members\": [\n"
                                                    + "            {\n"
                                                    + "              \"identifier\": \"1\",\n"
                                                    + "              \"data_points\": [\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T03:00:00Z\",\n"
                                                    + "                  \"value\": \"270.9899939429015\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T02:00:00Z\",\n"
                                                    + "                  \"value\": \"334.139992531389\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T01:00:00Z\",\n"
                                                    + "                  \"value\": \"382.27999145537615\"\n"
                                                    + "                }\n"
                                                    + "              ]\n"
                                                    + "            }\n"
                                                    + "          ]\n"
                                                    + "        }\n"
                                                    + "      ]\n"
                                                    + "    }\n"
                                                    + "  ]\n"
                                                    + "}\n";

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

            Geometry geometry = MessageUtilities.getGeometry( Integer.toString( NWM_FEATURE_ID ),
                                                              null,
                                                              null,
                                                              null );

            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                                                         Instant.parse( "2020-01-12T00:00:00Z" ) ),
                                                                 null,
                                                                 "streamflow",
                                                                 Feature.of( geometry ),
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

            Geometry geometry = MessageUtilities.getGeometry( Integer.toString( NWM_FEATURE_ID ),
                                                              null,
                                                              null,
                                                              null );

            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.ANALYSIS_START_TIME,
                                                                         Instant.parse( "2020-01-12T00:00:00Z" ) ),
                                                                 null,
                                                                 "streamflow",
                                                                 Feature.of( geometry ),
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

    @Test
    void testReadReturnsThreeChunkedForecastTimeSeries()
    {
        // Create the chunk parameters. Note the fiddly interval notation on the date ranges
        // First chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                  .withQueryParam( "proj", WireMock.equalTo( "UNKNOWN_PROJECT_USING_WRES" ) )
                                  .withQueryParam( "reference_time",
                                                   WireMock.equalTo( "(20220102T00Z,20220109T00Z]" ) )
                                  .withQueryParam( "forecast_type", WireMock.equalTo( "deterministic" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withBody( FORECAST_RESPONSE ) ) );

        // Second chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                  .withQueryParam( "proj", WireMock.equalTo( "UNKNOWN_PROJECT_USING_WRES" ) )
                                  .withQueryParam( "reference_time",
                                                   WireMock.equalTo( "(20220109T00Z,20220116T00Z]" ) )
                                  .withQueryParam( "forecast_type", WireMock.equalTo( "deterministic" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withBody( FORECAST_RESPONSE ) ) );

        // Third chunk
        WIREMOCK.stubFor( WireMock.get( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                  .withQueryParam( "proj", WireMock.equalTo( "UNKNOWN_PROJECT_USING_WRES" ) )
                                  .withQueryParam( "reference_time",
                                                   WireMock.equalTo( "(20220116T00Z,20220123T00Z]" ) )
                                  .withQueryParam( "forecast_type", WireMock.equalTo( "deterministic" ) )
                                  .willReturn( WireMock.aResponse()
                                                       .withStatus( 200 )
                                                       .withBody( FORECAST_RESPONSE ) ) );

        // Need to use a short URL, as would be declared, since chunking goes through URL creation
        URI fakeUri = URI.create( "http://localhost:"
                                  + WIREMOCK.getPort()
                                  + "/api/v1/nwm/ops/short_range/" );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
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
                                                          .setName( Integer.toString( NWM_FEATURE_ID ) ) )
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

            // Three chunks expected
            assertEquals( 3, actual.size() );
        }

        // Three requests made
        WIREMOCK.verify( WireMock.exactly( 3 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                 .withQueryParam( "proj", WireMock.equalTo( "UNKNOWN_PROJECT_USING_WRES" ) )
                                 .withQueryParam( "reference_time",
                                                  WireMock.equalTo( "(20220102T00Z,20220109T00Z]" ) )
                                 .withQueryParam( "forecast_type", WireMock.equalTo( "deterministic" ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                 .withQueryParam( "proj", WireMock.equalTo( "UNKNOWN_PROJECT_USING_WRES" ) )
                                 .withQueryParam( "reference_time",
                                                  WireMock.equalTo( "(20220109T00Z,20220116T00Z]" ) )
                                 .withQueryParam( "forecast_type", WireMock.equalTo( "deterministic" ) ) );

        WIREMOCK.verify( WireMock.exactly( 1 ),
                         WireMock.getRequestedFor( WireMock.urlPathEqualTo( FORECAST_PATH ) )
                                 .withQueryParam( "proj", WireMock.equalTo( "UNKNOWN_PROJECT_USING_WRES" ) )
                                 .withQueryParam( "reference_time",
                                                  WireMock.equalTo( "(20220116T00Z,20220123T00Z]" ) )
                                 .withQueryParam( "forecast_type", WireMock.equalTo( "deterministic" ) ) );
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
                                                          .setName( Integer.toString( NWM_FEATURE_ID ) ) )
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
                                                          .setName( Integer.toString( NWM_FEATURE_ID ) ) )
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