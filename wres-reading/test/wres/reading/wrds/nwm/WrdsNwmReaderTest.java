package wres.reading.wrds.nwm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.model.HttpRequest.request;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpError;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;
import org.mockserver.model.Parameters;
import org.mockserver.verify.VerificationTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DataType;
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
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.reading.ReadException;
import wres.statistics.MessageFactory;
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

    /** Mocker server instance. */
    private ClientAndServer mockServer;

    /** Feature considered. */
    private static final int NWM_FEATURE_ID = 8588002;

    /** Path used by GET for analysis. */
    private static final String ANALYSIS_PATH = "/api/v1/nwm/ops/analysis_assim/streamflow/nwm_feature_id/"
                                                + NWM_FEATURE_ID
                                                + "/";

    /** Parameters added to path. */
    private static final String ANALYSIS_PARAMS = "?reference_time=20200112T00Z";

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
                                                    + "      \"reference_time\": \"20200112T00Z\",\n"
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
                                                    + "                  \"time\": \"20200112T03Z\",\n"
                                                    + "                  \"value\": \"270.9899939429015\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T02Z\",\n"
                                                    + "                  \"value\": \"334.139992531389\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T01Z\",\n"
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
                                                    + "      \"reference_time\": \"20200112T00Z\",\n"
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
                                                    + "                  \"time\": \"20200112T03Z\",\n"
                                                    + "                  \"value\": \"270.9899939429015\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T02Z\",\n"
                                                    + "                  \"value\": \"334.139992531389\"\n"
                                                    + "                },\n"
                                                    + "                {\n"
                                                    + "                  \"time\": \"20200112T01Z\",\n"
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
    void testReadReturnsOneAnalysisTimeSeries()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( ANALYSIS_PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( ANALYSIS_RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + ANALYSIS_PATH
                                  + ANALYSIS_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        DataSource fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_NWM,
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

        WrdsNwmReader reader = WrdsNwmReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Geometry geometry = wres.statistics.MessageFactory.getGeometry( Integer.toString( NWM_FEATURE_ID ),
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

        this.mockServer.when( HttpRequest.request()
                                         .withPath( ANALYSIS_PATH )
                                         .withMethod( GET ),
                              Times.exactly( 2 ) )
                       .error( HttpError.error()
                                        .withDropConnection( true ) );

        // On the third time, return the body successfully.
        this.mockServer.when( HttpRequest.request()
                                         .withPath( ANALYSIS_PATH )
                                         .withMethod( "GET" ),
                              Times.once() )
                       .respond( org.mockserver.model.HttpResponse.response( ANALYSIS_RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + ANALYSIS_PATH
                                  + ANALYSIS_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .build();

        DataSource fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_NWM,
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

        WrdsNwmReader reader = WrdsNwmReader.of( systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

            Geometry geometry = MessageFactory.getGeometry( Integer.toString( NWM_FEATURE_ID ),
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
        Parameters parametersOne = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                   new Parameter( "reference_time",
                                                                  "(20220102T00Z,20220109T00Z]" ),
                                                   new Parameter( "forecast_type", "deterministic" ) );

        Parameters parametersTwo = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                   new Parameter( "reference_time",
                                                                  "(20220109T00Z,20220116T00Z]" ),
                                                   new Parameter( "forecast_type", "deterministic" ) );

        Parameters parametersThree = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                     new Parameter( "reference_time",
                                                                    "(20220116T00Z,20220123T00Z]" ),
                                                     new Parameter( "forecast_type", "deterministic" ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( FORECAST_PATH )
                                         .withQueryStringParameters( parametersOne )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( FORECAST_RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( FORECAST_PATH )
                                         .withQueryStringParameters( parametersTwo )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( FORECAST_RESPONSE ) );

        this.mockServer.when( HttpRequest.request()
                                         .withPath( FORECAST_PATH )
                                         .withQueryStringParameters( parametersThree )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( FORECAST_RESPONSE ) );

        // Need to use a short URL, as would be declared, since chunking goes through URL creation
        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
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

        DataSource fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_NWM,
                                               fakeDeclarationSource,
                                               dataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.RIGHT,
                                               null );

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
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( FORECAST_PATH ),
                                VerificationTimes.exactly( 3 ) );

        // One request made with parameters one
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( FORECAST_PATH )
                                         .withQueryStringParameters( parametersOne ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters two
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( FORECAST_PATH )
                                         .withQueryStringParameters( parametersTwo ),
                                VerificationTimes.exactly( 1 ) );

        // One request made with parameters three
        this.mockServer.verify( request().withMethod( GET )
                                         .withPath( FORECAST_PATH )
                                         .withQueryStringParameters( parametersThree ),
                                VerificationTimes.exactly( 1 ) );
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

        DataSource fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_NWM,
                                               fakeDeclarationSource,
                                               dataset,
                                               Collections.emptyList(),
                                               fakeUri,
                                               DatasetOrientation.RIGHT,
                                               null );

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
    @Disabled
    void testReadRequestsDateRangeWithMinutesAndSeconds()
    {
        this.mockServer.when( HttpRequest.request()
                                         .withPath( ANALYSIS_PATH )
                                         .withMethod( GET ) )
                       .respond( HttpResponse.response( ANALYSIS_RESPONSE ) );

        URI fakeUri = URI.create( "http://localhost:"
                                  + this.mockServer.getLocalPort()
                                  + "/api/v1/nwm/ops/analysis_assim/"
                                  + ANALYSIS_PARAMS );

        Source fakeDeclarationSource = SourceBuilder.builder()
                                                    .uri( fakeUri )
                                                    .sourceInterface( SourceInterface.WRDS_NWM )
                                                    .build();

        Dataset dataset = DatasetBuilder.builder()
                                        .sources( List.of( fakeDeclarationSource ) )
                                        .variable( VariableBuilder.builder()
                                                                  .name( "streamflow" )
                                                                  .build() )
                                        .build();

        DataSource fakeSource = DataSource.of( DataSource.DataDisposition.JSON_WRDS_NWM,
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

        // Create a declaration with a short period of valid dates to create one chunked request where the first and
        // last datetimes both contain minutes and seconds
        Instant minimum = Instant.parse( "2024-09-01T00:13:00Z" );
        Instant maximum = Instant.parse( "2024-09-03T00:27:59Z" );
        TimeInterval interval = TimeIntervalBuilder.builder()
                                                   .minimum( minimum )
                                                   .maximum( maximum )
                                                   .build();
        Set<GeometryTuple> geometries
                = Set.of( GeometryTuple.newBuilder()
                                       .setLeft( Geometry.newBuilder()
                                                         .setName( Integer.toString( NWM_FEATURE_ID ) ) )
                                       .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .validDates( interval )
                                                                        .features( features )
                                                                        .build();

        WrdsNwmReader reader = WrdsNwmReader.of( declaration, systemSettings );

        Parameters parametersOne = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                   new Parameter( "reference_time",
                                                                  "(20240901T000000Z,20240903T002759Z]" ),
                                                   new Parameter( "forecast_type", "deterministic" ) );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .toList();

            assertEquals( 1, actual.size() );

            // One request made with parameters one
            this.mockServer.verify( request().withMethod( GET )
                                             .withPath( ANALYSIS_PATH )
                                             .withQueryStringParameters( parametersOne ),
                                    VerificationTimes.exactly( 1 ) );
        }
    }
}