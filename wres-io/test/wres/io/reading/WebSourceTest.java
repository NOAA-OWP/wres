package wres.io.reading;

import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.ProjectConfig;
import wres.config.generated.UrlParameter;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

public class WebSourceTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebSourceTest.class );
    private static final URI BASE_URI = URI.create( "https://host/path" );
    private static final Instant START_INSTANT = Instant.parse( "2020-08-27T05:23:27Z" );
    private static final Instant END_INSTANT = Instant.parse( "2020-08-28T13:19:53Z" );
    private static final String FEATURE_NAME_ONE = "FEATURE1";
    private static final String FEATURE_NAME_TWO = "FEATURE2";
    private static final String URL_PARAM_NAME_ONE = "NameOne";
    private static final String URL_PARAM_VALUE_ONE = "ValueOne";
    private static final String URL_PARAM_NAME_TWO = "NameTwo";
    private static final String URL_PARAM_VALUE_TWO = "ValueTwo";
    @Mock private SystemSettings mockSystemSettings;
    @Mock private Database mockDatabase;
    @Mock private DataSources mockDataSourcesCache;
    @Mock private Features mockFeaturesCache;
    @Mock private Variables variablesCache;
    @Mock private Ensembles ensemblesCache;
    @Mock private MeasurementUnits measurementUnitsCache;
    private ProjectConfig fakeProjectConfig;
    @Mock private DatabaseLockManager lockManager;

    @Before
    public void setup()
    {
        MockitoAnnotations.openMocks( this );
        Mockito.when( mockSystemSettings.getMaximumWebClientThreads() )
               .thenReturn( 1 );
        this.fakeProjectConfig = new ProjectConfig( null, null, null, null, null, null );
    }

    @Test
    public void wrdsNwmSourceAddsUserSpecifiedParameters()
    {
        UrlParameter urlParameterOne = new UrlParameter( URL_PARAM_NAME_ONE, URL_PARAM_VALUE_ONE );
        UrlParameter urlParameterTwo = new UrlParameter( URL_PARAM_NAME_TWO, URL_PARAM_VALUE_TWO );
        DataSourceConfig.Source source = new DataSourceConfig.Source( BASE_URI,
                                                                      InterfaceShortHand.WRDS_NWM,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null );
        DataSourceConfig sourceConfig = new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                              List.of( source ),
                                                              new DataSourceConfig.Variable( "streamflow", null ),
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              List.of( urlParameterOne, urlParameterTwo ),
                                                              null,
                                                              null,
                                                              null );
        DataSource dataSource = DataSource.of( source,
                                               sourceConfig,
                                               Collections.emptySet(),
                                               BASE_URI );
        WebSource webSource = WebSource.of( mockSystemSettings,
                                            mockDatabase,
                                            mockDataSourcesCache,
                                            mockFeaturesCache,
                                            variablesCache,
                                            ensemblesCache,
                                            measurementUnitsCache,
                                            fakeProjectConfig,
                                            dataSource,
                                            lockManager );
        URI generatedUri = webSource.createUri( source.getValue(),
                                                dataSource,
                                                Pair.of( START_INSTANT, END_INSTANT ),
                                                new String[] { FEATURE_NAME_ONE, FEATURE_NAME_TWO } );
        webSource.shutdownNow();
        String queryOne = URL_PARAM_NAME_ONE + "=" + URL_PARAM_VALUE_ONE;
        String queryTwo = URL_PARAM_NAME_TWO + "=" + URL_PARAM_VALUE_TWO;
        String generatedQueryParts = generatedUri.getQuery();
        String generatedPath = generatedUri.getPath();
        LOGGER.info( "Created this url: {} with query parts: {} with path: {}",
                     generatedUri, generatedQueryParts, generatedPath );
        assertTrue( generatedQueryParts.contains( queryOne ) );
        assertTrue( generatedQueryParts.contains( queryTwo ) );
        assertTrue( generatedPath.contains( FEATURE_NAME_ONE ) );
        assertTrue( generatedPath.contains( FEATURE_NAME_TWO ) );
    }


    @Test
    public void wrdsAhpsSourceAddsUserSpecifiedParameters()
    {
        UrlParameter urlParameterOne = new UrlParameter( URL_PARAM_NAME_ONE, URL_PARAM_VALUE_ONE );
        UrlParameter urlParameterTwo = new UrlParameter( URL_PARAM_NAME_TWO, URL_PARAM_VALUE_TWO );
        DataSourceConfig.Source source = new DataSourceConfig.Source( BASE_URI,
                                                                      InterfaceShortHand.WRDS_AHPS,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null );
        DataSourceConfig sourceConfig = new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                              List.of( source ),
                                                              new DataSourceConfig.Variable( "QR", null ),
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              List.of( urlParameterOne, urlParameterTwo ),
                                                              null,
                                                              null,
                                                              null );
        DataSource dataSource = DataSource.of( source,
                                               sourceConfig,
                                               Collections.emptySet(),
                                               BASE_URI );
        WebSource webSource = WebSource.of( mockSystemSettings,
                                            mockDatabase,
                                            mockDataSourcesCache,
                                            mockFeaturesCache,
                                            variablesCache,
                                            ensemblesCache,
                                            measurementUnitsCache,
                                            fakeProjectConfig,
                                            dataSource,
                                            lockManager );
        URI generatedUri = webSource.createUri( source.getValue(),
                                                dataSource,
                                                Pair.of( START_INSTANT, END_INSTANT ),
                                                FEATURE_NAME_ONE );
        webSource.shutdownNow();
        String queryOne = URL_PARAM_NAME_ONE + "=" + URL_PARAM_VALUE_ONE;
        String queryTwo = URL_PARAM_NAME_TWO + "=" + URL_PARAM_VALUE_TWO;
        String generatedQueryParts = generatedUri.getQuery();
        String generatedPath = generatedUri.getPath();
        LOGGER.info( "Created this url: {} with query parts: {} with path: {}",
                     generatedUri, generatedQueryParts, generatedPath );
        assertTrue( generatedQueryParts.contains( queryOne ) );
        assertTrue( generatedQueryParts.contains( queryTwo ) );
        assertTrue( generatedPath.contains( FEATURE_NAME_ONE ) );
    }


    @Test
    public void usgsNwisSourceAddsUserSpecifiedParameters()
    {
        UrlParameter urlParameterOne = new UrlParameter( URL_PARAM_NAME_ONE, URL_PARAM_VALUE_ONE );
        UrlParameter urlParameterTwo = new UrlParameter( URL_PARAM_NAME_TWO, URL_PARAM_VALUE_TWO );
        DataSourceConfig.Source source = new DataSourceConfig.Source( BASE_URI,
                                                                      InterfaceShortHand.USGS_NWIS,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null );
        DataSourceConfig sourceConfig = new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                              List.of( source ),
                                                              new DataSourceConfig.Variable( "00060", null ),
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              List.of( urlParameterOne, urlParameterTwo ),
                                                              null,
                                                              null,
                                                              null );
        DataSource dataSource = DataSource.of( source,
                                               sourceConfig,
                                               Collections.emptySet(),
                                               BASE_URI );
        WebSource webSource = WebSource.of( mockSystemSettings,
                                            mockDatabase,
                                            mockDataSourcesCache,
                                            mockFeaturesCache,
                                            variablesCache,
                                            ensemblesCache,
                                            measurementUnitsCache,
                                            fakeProjectConfig,
                                            dataSource,
                                            lockManager );
        URI generatedUri = webSource.createUri( source.getValue(),
                                                dataSource,
                                                Pair.of( START_INSTANT, END_INSTANT ),
                                                new String[] { FEATURE_NAME_ONE, FEATURE_NAME_TWO } );
        webSource.shutdownNow();
        String queryOne = URL_PARAM_NAME_ONE + "=" + URL_PARAM_VALUE_ONE;
        String queryTwo = URL_PARAM_NAME_TWO + "=" + URL_PARAM_VALUE_TWO;
        String generatedQueryParts = generatedUri.getQuery();
        String generatedPath = generatedUri.getPath();
        LOGGER.info( "Created this url: {} with query parts: {} with path: {}",
                     generatedUri, generatedQueryParts, generatedPath );
        assertTrue( generatedQueryParts.contains( queryOne ) );
        assertTrue( generatedQueryParts.contains( queryTwo ) );
        assertTrue( generatedQueryParts.contains( FEATURE_NAME_ONE ) );
        assertTrue( generatedQueryParts.contains( FEATURE_NAME_TWO ) );
    }
}
