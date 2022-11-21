package wres.io.reading.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.model.HttpRequest.request;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.NamedFeature;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.ReadException;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.system.SystemSettings;

/**
 * Tests the {@link WrdsNwmReader}.
 * @author James Brown
 */

class WrdsNwmReaderTest
{
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
                                                    + "}";

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
                                                    + "}";

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

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_NWM,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_NWM,
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
                                               fakeUri,
                                               LeftOrRightOrBaseline.LEFT );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
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
                                                                 FeatureKey.of( geometry ),
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

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_NWM,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_NWM,
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
                                               fakeUri,
                                               LeftOrRightOrBaseline.LEFT );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
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
                                                                 FeatureKey.of( geometry ),
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
                                                   new Parameter( "forecast_type", "deterministic" ),
                                                   new Parameter( "validTime", "all" ) );

        Parameters parametersTwo = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                   new Parameter( "reference_time",
                                                                  "(20220109T00Z,20220116T00Z]" ),
                                                   new Parameter( "forecast_type", "deterministic" ),
                                                   new Parameter( "validTime", "all" ) );

        Parameters parametersThree = new Parameters( new Parameter( "proj", "UNKNOWN_PROJECT_USING_WRES" ),
                                                     new Parameter( "reference_time",
                                                                    "(20220116T00Z,20220123T00Z]" ),
                                                     new Parameter( "forecast_type", "deterministic" ),
                                                     new Parameter( "validTime", "all" ) );

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

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_NWM,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_NWM,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "streamflow", null ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               fakeUri,
                                               LeftOrRightOrBaseline.RIGHT );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                List.of( new NamedFeature( null,
                                                                      Integer.toString( NWM_FEATURE_ID ),
                                                                      null ) ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                new DateCondition( "2022-01-03T00:00:00Z", "2022-01-23T00:00:00Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        WrdsNwmReader reader = WrdsNwmReader.of( pairConfig, systemSettings );

        try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
        {
            List<TimeSeries<Double>> actual = tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                                                         .collect( Collectors.toList() );

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

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( fakeUri,
                                             InterfaceShortHand.WRDS_NWM,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_NWM,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "streamflow", null ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               fakeUri,
                                               LeftOrRightOrBaseline.RIGHT );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                List.of( new NamedFeature( null,
                                                                      Integer.toString( NWM_FEATURE_ID ),
                                                                      null ),
                                                         new NamedFeature( null,
                                                                      "234442421",
                                                                      null ) ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                new DateCondition( "2022-01-03T00:00:00Z", "2022-01-23T00:00:00Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        Mockito.when( systemSettings.getMaximumWebClientThreads() )
               .thenReturn( 6 );
        Mockito.when( systemSettings.poolObjectLifespan() )
               .thenReturn( 30_000 );

        // Feature chunk size of 1, with 2 features requested
        WrdsNwmReader reader = WrdsNwmReader.of( pairConfig, systemSettings, 1 );

        // Expect a ReadException due to the fake uri, not a ClassCastException as in #109238.
        assertThrows( ReadException.class, () -> {
            try ( Stream<TimeSeriesTuple> tupleStream = reader.read( fakeSource ) )
            {
                tupleStream.map( TimeSeriesTuple::getSingleValuedTimeSeries )
                           .collect( Collectors.toList() );
            }
        } );
    }

}
