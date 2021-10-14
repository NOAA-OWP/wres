package wres.io.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

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
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.FeaturePool;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.io.concurrency.Executor;
import wres.io.data.caching.Features;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.utilities.DataScripter;
import wres.io.utilities.TestDatabase;
import wres.system.SystemSettings;

/**
 * Tests the {@link Project}.
 * 
 * @author James Brown
 */

class ProjectTest
{

    private static final FeatureKey FEATURE = FeatureKey.of( "F" );
    private static final FeatureKey ANOTHER_FEATURE = FeatureKey.of( "G" );
    private static final String PROJECT_HASH = "881hfEaffja267";
    private static final String UNITS = "CFS";
    private static final String T2023_04_01T00_00_00Z = "2023-04-01T00:00:00Z";
    private static final String VARIABLE_NAME = "V";

    @Mock
    private SystemSettings mockSystemSettings;
    @Mock
    private Executor mockExecutor;

    private wres.io.utilities.Database wresDatabase;
    private Features featuresCache;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;
    private Project project;

    @BeforeAll
    static void oneTimeSetup()
    {
        // Set the JVM timezone for use by H2. Needs to happen before anything else
        TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
    }

    @BeforeEach
    void RunBeforeEachTest() throws SQLException, LiquibaseException
    {
        MockitoAnnotations.openMocks( this );

        // Create the database and connection pool
        this.testDatabase = new TestDatabase( "SingleValuedForecastRetrieverTest" );
        this.dataSource = this.testDatabase.getNewHikariDataSource();

        // Create the connection and schema
        this.rawConnection = DriverManager.getConnection( this.testDatabase.getJdbcString() );
        this.testDatabase.createWresSchema( this.rawConnection );

        // Substitute our H2 connection pool for both pools:
        Mockito.when( this.mockSystemSettings.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseType() )
               .thenReturn( "h2" );
        Mockito.when( this.mockSystemSettings.getMaximumPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.utilities.Database( this.mockSystemSettings );
        this.featuresCache = new Features( this.wresDatabase );

        // Create the tables
        this.addTheDatabaseAndTables();

        // Get a project for testing, backed by data
        ProjectConfig projectConfig = this.getProjectConfig();
        this.project = this.getProject( projectConfig );
    }

    @AfterEach
    void runAfterEachTest() throws SQLException
    {
        this.dropTheTablesAndSchema();
        this.rawConnection.close();
        this.rawConnection = null;
        this.testDatabase = null;
        this.dataSource.close();
        this.dataSource = null;
    }

    @Test
    void testGetFeatures() throws SQLException
    {
        this.project.prepareForExecution();

        Set<FeatureTuple> actual = this.project.getFeatures();
        Set<FeatureTuple> expected = Set.of( new FeatureTuple( FEATURE, FEATURE, null ),
                                             new FeatureTuple( ANOTHER_FEATURE, ANOTHER_FEATURE, null ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetFeatureGroups() throws SQLException
    {
        this.project.prepareForExecution();

        Set<FeatureGroup> actual = this.project.getFeatureGroups();

        FeatureGroup firstGroup = FeatureGroup.of( "F-F", new FeatureTuple( FEATURE, FEATURE, null ) );
        FeatureGroup secondGroup = FeatureGroup.of( "G-G",
                                                    new FeatureTuple( ANOTHER_FEATURE, ANOTHER_FEATURE, null ) );
        FeatureGroup overallGroup = FeatureGroup.of( "A feature group!",
                                                     Set.of( new FeatureTuple( FEATURE, FEATURE, null ),
                                                             new FeatureTuple( ANOTHER_FEATURE,
                                                                               ANOTHER_FEATURE,
                                                                               null ) ) );

        Set<FeatureGroup> expected = Set.of( firstGroup, secondGroup, overallGroup );

        assertEquals( expected, actual );
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

        this.testDatabase.createMeasurementUnitTable( liquibaseDatabase );
        this.testDatabase.createSourceTable( liquibaseDatabase );
        this.testDatabase.createProjectTable( liquibaseDatabase );
        this.testDatabase.createProjectSourceTable( liquibaseDatabase );
        this.testDatabase.createFeatureTable( liquibaseDatabase );
        this.testDatabase.createEnsembleTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesValueTable( liquibaseDatabase );
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

    private Project getProject( ProjectConfig projectConfig ) throws SQLException
    {
        // Add a source
        SourceDetails.SourceKey sourceKey = SourceDetails.createKey( URI.create( "/this/is/just/a/test" ),
                                                                     "2017-06-16 11:13:00",
                                                                     null,
                                                                     "abc123" );

        SourceDetails sourceDetails = new SourceDetails( sourceKey );

        sourceDetails.save( this.wresDatabase );

        assertTrue( sourceDetails.performedInsert() );

        Long sourceId = sourceDetails.getId();

        assertNotNull( sourceId );

        // Add a project 
        Project project =
                new Project( this.mockSystemSettings,
                             this.wresDatabase,
                             this.featuresCache,
                             this.mockExecutor,
                             projectConfig,
                             PROJECT_HASH );
        project.save();

        assertTrue( project.performedInsert() );

        assertEquals( PROJECT_HASH, project.getHash() );

        // Add the same project source to each side
        // There is no wres abstraction to help with this
        String rightSourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ({0},{1},''{2}'')";

        rightSourceInsert = MessageFormat.format( rightSourceInsert,
                                                  project.getId(),
                                                  sourceId,
                                                  LeftOrRightOrBaseline.RIGHT.value() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                rightSourceInsert );
        int rows = script.execute();

        assertEquals( 1, rows );

        String leftSourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ({0},{1},''{2}'')";

        leftSourceInsert = MessageFormat.format( leftSourceInsert,
                                                 project.getId(),
                                                 sourceId,
                                                 LeftOrRightOrBaseline.LEFT.value() );

        DataScripter leftScript = new DataScripter( this.wresDatabase,
                                                    leftSourceInsert );
        int rowsLeft = leftScript.execute();

        assertEquals( 1, rowsLeft );

        // Add two features
        FeatureDetails feature = new FeatureDetails( FEATURE );
        feature.save( this.wresDatabase );
        assertNotNull( feature.getId() );

        FeatureDetails anotherFeature = new FeatureDetails( ANOTHER_FEATURE );
        anotherFeature.save( this.wresDatabase );
        assertNotNull( anotherFeature.getId() );

        // Get the measurement units for CFS
        MeasurementDetails measurement = new MeasurementDetails();

        measurement.setUnit( UNITS );
        measurement.save( this.wresDatabase );
        Long measurementUnitId = measurement.getId();

        assertNotNull( measurementUnitId );

        EnsembleDetails ensemble = new EnsembleDetails();
        ensemble.setEnsembleName( "ENS123" );
        ensemble.save( this.wresDatabase );
        Long ensembleId = ensemble.getId();

        assertNotNull( ensembleId );

        // Add two forecasts
        // Two reference times, PT17H apart
        Instant firstReference = Instant.parse( T2023_04_01T00_00_00Z );

        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN );

        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     firstReference,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     feature.getId() );
        firstTraceRow.setTimeScale( timeScale );
        // Do the save
        firstTraceRow.getTimeSeriesID();

        wres.io.data.details.TimeSeries secondTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     measurementUnitId,
                                                     firstReference,
                                                     sourceId,
                                                     VARIABLE_NAME,
                                                     anotherFeature.getId() );
        secondTraceRow.setTimeScale( timeScale );
        // Do the save
        secondTraceRow.getTimeSeriesID();

        return project;
    }

    /**
     * @return some project declaration
     */

    private ProjectConfig getProjectConfig()
    {
        String featureName = FEATURE.getName();
        String anotherName = ANOTHER_FEATURE.getName();
        List<Feature> features =
                List.of( new Feature( featureName, featureName, null ), new Feature( anotherName, anotherName, null ) );
        List<FeaturePool> featureGroups = List.of( new FeaturePool( features, "A feature group!" ) );

        Inputs inputs = new Inputs( new DataSourceConfig( DatasourceType.OBSERVATIONS,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null ),
                                    new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null,
                                                          null ),
                                    null );

        return new ProjectConfig( inputs,
                                  new PairConfig( null,
                                                  null,
                                                  null,
                                                  features,
                                                  featureGroups,
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
                                                  null ),
                                  null,
                                  null,
                                  null,
                                  "test_project" );
    }

}
