package wres.io.project;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.zaxxer.hikari.HikariDataSource;

import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.types.Ensemble;
import wres.io.database.ConnectionSupplier;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.TestDatabase;
import wres.io.database.locking.DatabaseLockManager;
import wres.io.database.locking.DatabaseLockManagerNoop;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.TestData;
import wres.reading.DataSource;
import wres.reading.TimeSeriesTuple;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link DatabaseProject}.
 *
 * @author James Brown
 */
class DatabaseProjectTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseProjectTest.class );
    private static final Feature FEATURE = Feature.of(
            wres.statistics.MessageFactory.getGeometry( "F" ) );
    @Mock
    private SystemSettings mockSystemSettings;

    @Mock
    private ConnectionSupplier mockConnectionSupplier;

    private wres.io.database.Database wresDatabase;

    @Mock
    private DatabaseSettings mockDatabaseSettings;

    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;
    private DatabaseProject project;
    private AutoCloseable mocks;
    private ExecutorService ingestExecutor;
    private DatabaseCaches caches;
    private DatabaseLockManager lockManager;

    @BeforeAll
    static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @BeforeEach
    void RunBeforeEachTest() throws SQLException, LiquibaseException
    {
        this.mocks = MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( this.getClass().getName() );
        this.dataSource = this.testDatabase.getNewHikariDataSource();
        this.ingestExecutor = DatabaseProjectTest.getIngestExecutor();

        // Create the connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockConnectionSupplier.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockConnectionSupplier.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockConnectionSupplier.getSystemSettings() )
               .thenReturn( this.mockSystemSettings );
        Mockito.when( this.mockSystemSettings.getDatabaseConfiguration() )
               .thenReturn( mockDatabaseSettings );
        Mockito.when( this.mockDatabaseSettings.getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockDatabaseSettings.getMaxPoolSize() )
               .thenReturn( 10 );
        Mockito.when( this.mockSystemSettings.getMaximumThreadCount() )
               .thenReturn( 7 );
        Mockito.when( this.mockSystemSettings.getMaximumIngestThreads() )
               .thenReturn( 7 );

        this.wresDatabase = new wres.io.database.Database( this.mockConnectionSupplier );
        this.caches = DatabaseCaches.of( this.wresDatabase );
        this.lockManager = new DatabaseLockManagerNoop();

        // Create the tables
        this.addTheDatabaseAndTables();

        // Get a project for testing, backed by data
        this.project = this.getProject();
    }

    @AfterEach
    void runAfterEachTest() throws Exception
    {
        this.dropTheTablesAndSchema();
        this.rawConnection.close();
        this.rawConnection = null;
        this.testDatabase = null;
        this.dataSource.close();
        this.dataSource = null;

        if ( Objects.nonNull( mocks ) )
        {
            this.mocks.close();
        }
    }

    @Test
    void testGetFeatures()
    {
        Set<FeatureTuple> actual = this.project.getFeatures();

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, null );
        FeatureTuple aTuple = FeatureTuple.of( geoTuple );

        Set<FeatureTuple> expected = Set.of( aTuple );

        assertEquals( expected, actual );
    }

    @Test
    void testGetFeatureGroups()
    {
        Set<FeatureGroup> actual = this.project.getFeatureGroups();

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, null );
        FeatureTuple aTuple = FeatureTuple.of( geoTuple );

        FeatureGroup firstGroup = FeatureGroup.of( MessageFactory.getGeometryGroup( "F-F", aTuple ) );

        Set<FeatureGroup> expected = Set.of( firstGroup );

        // Assert equality: #103804
        assertEquals( expected, actual );
    }

    @Test
    void testGetEnsembleLabels()
    {
        SortedSet<String> actual = this.project.getEnsembleLabels( DatasetOrientation.RIGHT );
        assertEquals( new TreeSet<>( Set.of( "123", "456", "567" ) ), actual );
    }

    /**
     * Adds the required tables for the tests presented here, which is a subset of all tables.
     * @throws LiquibaseException if the tables could not be created
     */

    private void addTheDatabaseAndTables() throws LiquibaseException
    {
        // Create the required tables
        Database liquibaseDatabase =
                this.testDatabase.createNewLiquibaseDatabase( this.rawConnection );

        this.testDatabase.createAllTables( liquibaseDatabase );
    }

    /**
     * Drops the schema, cascading to all tables.
     * @throws SQLException if any tables or the schema failed to drop
     */
    private void dropTheTablesAndSchema() throws SQLException
    {
        this.testDatabase.dropWresSchema( this.rawConnection );
        this.testDatabase.dropLiquibaseChangeTables( this.rawConnection );
    }

    /**
     * @return a project for testing, backed by data
     * @throws SQLException if the detailed set-up fails
     */

    private DatabaseProject getProject() throws SQLException
    {
        DataSource leftData = TestData.generateDataSource( DatasetOrientation.LEFT,
                                                           DataType.OBSERVATIONS );
        DataSource rightData = TestData.generateDataSource( DatasetOrientation.RIGHT,
                                                            DataType.ENSEMBLE_FORECASTS );
        LOGGER.debug( "leftData: {}", leftData );
        LOGGER.debug( "rightData: {}", rightData );

        String featureName = FEATURE.getName();
        Geometry geometry = Geometry.newBuilder()
                                    .setName( featureName )
                                    .build();
        Set<GeometryTuple> features =
                Set.of( GeometryTuple.newBuilder()
                                     .setLeft( geometry )
                                     .setRight( geometry )
                                     .build() );

        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.ENSEMBLE_FORECASTS )
                                      .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .features( new wres.config.yaml.components.Features( features ) )
                                            .build();

        TimeSeries<Ensemble> timeSeriesOne = TestData.generateTimeSeriesEnsembleOne();
        DatabaseTimeSeriesIngester ingesterOne =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setIngestExecutor( this.ingestExecutor )
                                                        .setCaches( this.caches )
                                                        .setLockManager( this.lockManager )
                                                        .build();

        Stream<TimeSeriesTuple> tupleStreamOne =
                Stream.of( TimeSeriesTuple.ofEnsemble( timeSeriesOne, rightData ) );
        IngestResult ingestResultOne = ingesterOne.ingest( tupleStreamOne, rightData )
                                                  .get( 0 );

        TimeSeries<Double> timeSeriesTwo = TestData.generateTimeSeriesDoubleWithNoReferenceTimes();
        DatabaseTimeSeriesIngester ingesterTwo =
                new DatabaseTimeSeriesIngester.Builder().setSystemSettings( this.mockSystemSettings )
                                                        .setDatabase( this.wresDatabase )
                                                        .setIngestExecutor( this.ingestExecutor )
                                                        .setCaches( this.caches )
                                                        .setLockManager( this.lockManager )
                                                        .build();
        Stream<TimeSeriesTuple> tupleStreamTwo =
                Stream.of( TimeSeriesTuple.ofSingleValued( timeSeriesTwo, leftData ) );
        IngestResult ingestResultTwo = ingesterTwo.ingest( tupleStreamTwo, leftData )
                                                  .get( 0 );

        List<IngestResult> results = List.of( ingestResultOne,
                                              ingestResultTwo );

        try ( Statement statement = this.rawConnection.createStatement() )
        {
            ResultSet sourceData =
                    statement.executeQuery( "select source_id, hash, measurementunit_id, path from wres.source" );

            while ( sourceData.next() )
            {
                LOGGER.info( "source_id={} hash={} measurementunit_id={} path={}",
                             sourceData.getLong( "source_id" ),
                             sourceData.getString( "hash" ),
                             sourceData.getShort( "measurementunit_id" ),
                             sourceData.getString( "path" ) );
            }
        }

        LOGGER.info( "ingestResultOne: {}", ingestResultOne );
        LOGGER.info( "ingestResultTwo: {}", ingestResultTwo );
        return new DatabaseProject( this.wresDatabase,
                                    this.caches,
                                    null,
                                    declaration,
                                    results );
    }

    /**
     * @return an ingest executor
     */
    private static ExecutorService getIngestExecutor()
    {
        // Create an ingest executor
        ThreadFactory ingestFactory =
                new BasicThreadFactory.Builder().namingPattern( "Ingesting Thread %d" )
                                                .build();
        // Queue should be large enough to allow join() call to be reached with zero or few rejected submissions to the
        // executor service.
        BlockingQueue<Runnable> ingestQueue = new ArrayBlockingQueue<>( 7 );

        RejectedExecutionHandler ingestHandler = new ThreadPoolExecutor.CallerRunsPolicy();
        return new ThreadPoolExecutor( 7,
                                       7,
                                       30000,
                                       TimeUnit.MILLISECONDS,
                                       ingestQueue,
                                       ingestFactory,
                                       ingestHandler );
    }
}
