package wres.io.reading.web;

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
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.config.generated.UrlParameter;
import wres.io.data.caching.Caches;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.DataSource.DataDisposition;
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
    @Mock private Caches mockCaches;
    private ProjectConfig fakeProjectConfig;
    @Mock private DatabaseLockManager lockManager;
    @Mock TimeSeriesIngester timeSeriesIngester;

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
                                                                      null );
        DataSourceConfig sourceConfig = new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                              List.of( source ),
                                                              new DataSourceConfig.Variable( "streamflow", null ),
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              List.of( urlParameterOne, urlParameterTwo ),
                                                              null,
                                                              null );
        DataSource dataSource = DataSource.of( DataDisposition.JSON_WRDS_NWM,
                                               source,
                                               sourceConfig,
                                               Collections.emptyList(),
                                               BASE_URI,
                                               LeftOrRightOrBaseline.RIGHT );
        WebSource webSource = WebSource.of( this.timeSeriesIngester,
                                            this.mockSystemSettings,
                                            this.mockDatabase,
                                            this.mockCaches,
                                            this.fakeProjectConfig,
                                            dataSource,
                                            this.lockManager );
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
                                                                      null );
        DataSourceConfig sourceConfig = new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                              List.of( source ),
                                                              new DataSourceConfig.Variable( "QR", null ),
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              List.of( urlParameterOne, urlParameterTwo ),
                                                              null,
                                                              null );
        DataSource dataSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               source,
                                               sourceConfig,
                                               Collections.emptyList(),
                                               BASE_URI,
                                               LeftOrRightOrBaseline.RIGHT  );
        WebSource webSource = WebSource.of( this.timeSeriesIngester,
                                            this.mockSystemSettings,
                                            this.mockDatabase,
                                            this.mockCaches,
                                            this.fakeProjectConfig,
                                            dataSource,
                                            this.lockManager );
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
                                                                      null );
        DataSourceConfig sourceConfig = new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                              List.of( source ),
                                                              new DataSourceConfig.Variable( "00060", null ),
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              null,
                                                              List.of( urlParameterOne, urlParameterTwo ),
                                                              null,
                                                              null );
        DataSource dataSource = DataSource.of( DataDisposition.JSON_WATERML,
                                               source,
                                               sourceConfig,
                                               Collections.emptyList(),
                                               BASE_URI,
                                               LeftOrRightOrBaseline.LEFT  );
        WebSource webSource = WebSource.of( this.timeSeriesIngester,
                                            this.mockSystemSettings,
                                            this.mockDatabase,
                                            this.mockCaches,
                                            this.fakeProjectConfig,
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
