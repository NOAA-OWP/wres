package wres.io.reading;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpError;
import org.mockserver.model.HttpRequest;
import org.mockserver.verify.VerificationTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static wres.io.reading.DataSource.DataDisposition.COMPLEX;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

public class WrdsNwmReaderTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReaderTest.class );
    private static ClientAndServer mockServer;
    private static final int NWM_FEATURE_ID = 8588002;
    private static final String ANALYSIS_PATH = "/api/v1/nwm/ops/analysis_assim/streamflow/nwm_feature_id/"
                                                + NWM_FEATURE_ID + "/";
    private static final String ANALYSIS_PARAMS = "?reference_time=20200112T00Z";
    private static final String ANALYSIS_VALID_RESPONSE = "{\n"
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
            + NWM_FEATURE_ID + "\",\n"
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

    private SystemSettings systemSettings;
    @Mock private Database mockDatabase;
    @Mock private Features mockFeaturesCache;
    @Mock private Variables mockVariablesCache;
    @Mock private Ensembles mockEnsemblesCache;
    @Mock private MeasurementUnits mockMeasurementUnitsCache;
    @Mock private DatabaseLockManager fakeLockManager;

    @Mock
    private TimeSeriesIngester fakeTimeSeriesIngester;

    @Captor
    ArgumentCaptor<TimeSeries<Double>> timeSeries;

    @BeforeClass
    public static void createFakeServer()
    {
        WrdsNwmReaderTest.mockServer = startClientAndServer( 0 );
    }

    @Before
    public void init()
    {
        MockitoAnnotations.openMocks( this);
        this.systemSettings = SystemSettings.withDefaults();
    }

    @Test
    public void readAndSaveValidWrdsNwmTimeSeries() throws IOException
    {
        WrdsNwmReaderTest.mockServer.when( HttpRequest.request()
                                                      .withPath( ANALYSIS_PATH )
                                                      .withMethod( "GET" ) )
                                    .respond( org.mockserver.model.HttpResponse.response( ANALYSIS_VALID_RESPONSE ) );
        URI fakeWrdsUri = URI.create( "http://localhost:"
                                      + WrdsNwmReaderTest.mockServer.getLocalPort()
                                      + ANALYSIS_PATH
                                      + ANALYSIS_PARAMS );

        List<DataSourceConfig.Source> sourceList = new ArrayList<>( 1 );
        DataSourceConfig.Source confSource = new DataSourceConfig.Source( fakeWrdsUri,
                                                                          InterfaceShortHand.WRDS_NWM,
                                                                          null,
                                                                          null,
                                                                          null );
        sourceList.add( confSource );

        DataSourceConfig.Variable configVariable = new DataSourceConfig.Variable( "streamflow", null );
        DataSourceConfig config = new DataSourceConfig( DatasourceType.ANALYSES,
                                                        sourceList,
                                                        configVariable,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null );

        ProjectConfig.Inputs inputs = new ProjectConfig.Inputs( null,
                                                                config,
                                                                null );

        Feature featureConfig = new Feature( Integer.toString( NWM_FEATURE_ID ),
                                             Integer.toString( NWM_FEATURE_ID ),
                                             null );

        List<Feature> features = new ArrayList<>( 1 );
        features.add( featureConfig );
        PairConfig pairConfig = new PairConfig( "CMS",
                                                null,
                                                null,
                                                features,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        DataSource dataSource = DataSource.of( COMPLEX,
                                               confSource,
                                               config,
                                               List.of( LeftOrRightOrBaseline.LEFT,
                                                        LeftOrRightOrBaseline.RIGHT ),
                                               fakeWrdsUri );

        WrdsNwmReader reader = Mockito.spy( new WrdsNwmReader( this.systemSettings,
                                                               this.mockDatabase,
                                                               this.mockFeaturesCache,
                                                               this.mockVariablesCache,
                                                               this.mockEnsemblesCache,
                                                               this.mockMeasurementUnitsCache,
                                                               projectConfig,
                                                               dataSource,
                                                               this.fakeLockManager ) );

        Mockito.doReturn( this.fakeTimeSeriesIngester )
               .when( reader )
               .createTimeSeriesIngester( any( SystemSettings.class ),
                                          any( Database.class ),
                                          any( Features.class ),
                                          any( Variables.class ),
                                          any( Ensembles.class ),
                                          any( MeasurementUnits.class ),
                                          any( ProjectConfig.class ),
                                          any( DataSource.class ),
                                          any( DatabaseLockManager.class ),
                                          any( TimeSeries.class ) );


        // Exercise the reader by executing call method.
        // This is the actual test. Everything up to this point is setup.
        reader.call();



        // Assertions, verify.

        // Capture the argument to TimeSeriesIngester.
        // Probably should refactor WrdsNwmReader to avoid having to do this.
        Mockito.verify( reader )
               .createTimeSeriesIngester( any( SystemSettings.class ),
                                          any( Database.class ),
                                          any( Features.class ),
                                          any( Variables.class ),
                                          any( Ensembles.class ),
                                          any( MeasurementUnits.class ),
                                          any( ProjectConfig.class ),
                                          any( DataSource.class ),
                                          any( DatabaseLockManager.class ),
                                          this.timeSeries.capture() );

        // Verify that the reader requested analysis data path given to it once.
        WrdsNwmReaderTest.mockServer.verify( request().withMethod( "GET" )
                                                      .withPath( ANALYSIS_PATH ),
                                             VerificationTimes.once() );

        // Verify that a TimeSeries was created and given to the Ingester.
        TimeSeries<Double> data = this.timeSeries.getValue();
        assertNotNull( data );

        LOGGER.info( "Created this timeseries: {}", data );

        // Verify that the TimeSeries content matched what was in the body.
        // See around lines 92 through 101 above (or last values in ANALYSIS_VALID_RESPONSE)

        Event<Double> expectedEventOne =
                Event.of( Instant.parse( "2020-01-12T03:00:00Z" ),
                          270.9899939429015 );
        Event<Double> expectedEventTwo =
                Event.of( Instant.parse( "2020-01-12T02:00:00Z" ),
                          334.139992531389 );
        Event<Double> expectedEventThree =
                Event.of( Instant.parse( "2020-01-12T01:00:00Z" ),
                          382.27999145537615 );

        assertTrue( data.getEvents()
                        .contains( expectedEventOne ) );
        assertTrue( data.getEvents()
                        .contains( expectedEventTwo ) );
        assertTrue( data.getEvents()
                        .contains( expectedEventThree ) );

        // Verify that only three events exist.
        assertEquals( 3, data.getEvents()
                             .size());

        // Verify that there is no T0 in analysis data
        for ( ReferenceTimeType time : data.getReferenceTimes()
                                           .keySet() )
        {
            assertNotEquals( ReferenceTimeType.T0, time );
        }
    }


    @Test
    public void readAndSaveValidWrdsTimeSeriesAfterTwoDroppedConnectionsFirst() throws IOException
    {
        WrdsNwmReaderTest.mockServer.when( HttpRequest.request()
                                                      .withPath( ANALYSIS_PATH )
                                                      .withMethod( "GET" ),
                                           Times.exactly( 2 ) )
                                    .error( HttpError.error()
                                                     .withDropConnection( true ) );

        // On the third time, return the body successfully.
        WrdsNwmReaderTest.mockServer.when( HttpRequest.request()
                                                      .withPath( ANALYSIS_PATH )
                                                      .withMethod( "GET" ),
                                           Times.once() )
                                    .respond( org.mockserver.model.HttpResponse.response(
                                            ANALYSIS_VALID_RESPONSE ) );


        URI fakeWrdsUri = URI.create( "http://localhost:"
                                      + WrdsNwmReaderTest.mockServer.getLocalPort()
                                      + ANALYSIS_PATH
                                      + ANALYSIS_PARAMS );

        List<DataSourceConfig.Source> sourceList = new ArrayList<>( 1 );
        DataSourceConfig.Source confSource =
                new DataSourceConfig.Source( fakeWrdsUri,
                                             InterfaceShortHand.WRDS_NWM,
                                             null,
                                             null,
                                             null );
        sourceList.add( confSource );

        DataSourceConfig.Variable configVariable =
                new DataSourceConfig.Variable( "streamflow", null );
        DataSourceConfig config = new DataSourceConfig( DatasourceType.ANALYSES,
                                                        sourceList,
                                                        configVariable,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null,
                                                        null );

        ProjectConfig.Inputs inputs = new ProjectConfig.Inputs( null,
                                                                config,
                                                                null );

        Feature featureConfig = new Feature( Integer.toString( NWM_FEATURE_ID ),
                                             Integer.toString( NWM_FEATURE_ID ),
                                             null );

        List<Feature> features = new ArrayList<>( 1 );
        features.add( featureConfig );
        PairConfig pairConfig = new PairConfig( "CMS",
                                                null,
                                                null,
                                                features,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( inputs,
                                                         pairConfig,
                                                         null,
                                                         null,
                                                         null,
                                                         null );

        DataSource dataSource = DataSource.of( COMPLEX,
                                               confSource,
                                               config,
                                               List.of( LeftOrRightOrBaseline.LEFT,
                                                        LeftOrRightOrBaseline.RIGHT ),
                                               fakeWrdsUri );

        WrdsNwmReader reader = Mockito.spy( new WrdsNwmReader( this.systemSettings,
                                                               this.mockDatabase,
                                                               this.mockFeaturesCache,
                                                               this.mockVariablesCache,
                                                               this.mockEnsemblesCache,
                                                               this.mockMeasurementUnitsCache,
                                                               projectConfig,
                                                               dataSource,
                                                               this.fakeLockManager ) );

        Mockito.doReturn( this.fakeTimeSeriesIngester )
               .when( reader )
               .createTimeSeriesIngester( any( SystemSettings.class ),
                                          any( Database.class ),
                                          any( Features.class ),
                                          any( Variables.class ),
                                          any( Ensembles.class ),
                                          any( MeasurementUnits.class ),
                                          any( ProjectConfig.class ),
                                          any( DataSource.class ),
                                          any( DatabaseLockManager.class ),
                                          any( TimeSeries.class ) );



        // Exercise the reader by executing call method.
        // This is the actual test. Everything up to this point is setup.
        reader.call();


        // Assertions, verify.

        // Capture the argument to TimeSeriesIngester.
        // Probably should refactor WrdsNwmReader to avoid having to do this.
        Mockito.verify( reader )
               .createTimeSeriesIngester( any( SystemSettings.class ),
                                          any( Database.class ),
                                          any( Features.class ),
                                          any( Variables.class ),
                                          any( Ensembles.class ),
                                          any( MeasurementUnits.class ),
                                          any( ProjectConfig.class ),
                                          any( DataSource.class ),
                                          any( DatabaseLockManager.class ),
                                          this.timeSeries.capture() );

        // Verify that the reader requested thrice (2 times failed).
        WrdsNwmReaderTest.mockServer.verify( request().withMethod( "GET" )
                                                      .withPath( ANALYSIS_PATH ),
                                             VerificationTimes.exactly( 3 ) );

        // Verify that a TimeSeries was created and given to the Ingester.
        TimeSeries<Double> data = this.timeSeries.getValue();
        assertNotNull( data );

        LOGGER.info( "Created this timeseries: {}", data );

        // Verify that the TimeSeries content matched what was in the body.
        // See around lines 92 through 101 above (or last values in ANALYSIS_VALID_RESPONSE)

        Event<Double> expectedEventOne =
                Event.of( Instant.parse( "2020-01-12T03:00:00Z" ),
                          270.9899939429015 );
        Event<Double> expectedEventTwo =
                Event.of( Instant.parse( "2020-01-12T02:00:00Z" ),
                          334.139992531389 );
        Event<Double> expectedEventThree =
                Event.of( Instant.parse( "2020-01-12T01:00:00Z" ),
                          382.27999145537615 );

        assertTrue( data.getEvents()
                        .contains( expectedEventOne ) );
        assertTrue( data.getEvents()
                        .contains( expectedEventTwo ) );
        assertTrue( data.getEvents()
                        .contains( expectedEventThree ) );

        // Verify that only three events exist.
        assertEquals( 3, data.getEvents()
                             .size() );

        // Verify that there is no T0 in analysis data
        for ( ReferenceTimeType time : data.getReferenceTimes()
                                           .keySet() )
        {
            assertNotEquals( ReferenceTimeType.T0, time );
        }
    }


    @After
    public void tearDown()
    {
        WrdsNwmReaderTest.mockServer.reset();
    }
}
