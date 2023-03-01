package wres.io.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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
import wres.config.generated.NamedFeature;
import wres.config.generated.FeaturePool;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.Features;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.MeasurementDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeScaleDetails;
import wres.io.database.DataScripter;
import wres.io.database.TestDatabase;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * Tests the {@link DatabaseProject}.
 * 
 * @author James Brown
 */

class DatabaseProjectTest
{
    private static final Feature FEATURE = Feature.of(
                                                             MessageFactory.getGeometry( "F" ) );
    private static final Feature ANOTHER_FEATURE = Feature.of(
                                                                     MessageFactory.getGeometry( "G" ) );
    private static final String PROJECT_HASH = "881hfEaffja267";
    private static final String UNITS = "[ft_i]3/s";
    private static final String VARIABLE_NAME = "V";

    @Mock
    private SystemSettings mockSystemSettings;

    private wres.io.database.Database wresDatabase;
    @Mock
    private DatabaseCaches mockCaches;
    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;
    private DatabaseProject project;

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
        this.testDatabase = new TestDatabase( this.getClass().getName() );
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
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockSystemSettings.getDatabaseMaximumPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.database.Database( this.mockSystemSettings );
        Features featuresCache = new Features( this.wresDatabase );

        Mockito.when( this.mockCaches.getFeaturesCache() )
               .thenReturn( featuresCache );

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
        this.project.prepareAndValidate();

        Set<FeatureTuple> actual = this.project.getFeatures();

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, null );
        FeatureTuple aTuple = FeatureTuple.of( geoTuple );
        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( ANOTHER_FEATURE, ANOTHER_FEATURE, null );
        FeatureTuple anotherTuple = FeatureTuple.of( anotherGeoTuple );

        Set<FeatureTuple> expected = Set.of( aTuple, anotherTuple );

        assertEquals( expected, actual );
    }

    @Test
    void testGetFeatureGroups() throws SQLException
    {
        this.project.prepareAndValidate();

        Set<FeatureGroup> actual = this.project.getFeatureGroups();

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, null );
        FeatureTuple aTuple = FeatureTuple.of( geoTuple );
        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( ANOTHER_FEATURE, ANOTHER_FEATURE, null );
        FeatureTuple anotherTuple = FeatureTuple.of( anotherGeoTuple );

        FeatureGroup firstGroup = FeatureGroup.of( MessageFactory.getGeometryGroup( "F-F", aTuple ) );
        FeatureGroup secondGroup = FeatureGroup.of( MessageFactory.getGeometryGroup( "G-G", anotherTuple ) );
        FeatureGroup overallGroup =
                FeatureGroup.of( MessageFactory.getGeometryGroup( "A feature group!",
                                                                  Set.of( aTuple, anotherTuple ) ) );

        Set<FeatureGroup> expected = Set.of( firstGroup, secondGroup, overallGroup );

        // Add some assertions to help diagnose #103804
        Assertions.assertAll( () -> assertEquals( 3, actual.size() ),
                              () -> assertTrue( actual.contains( firstGroup ) ),
                              () -> assertTrue( actual.contains( secondGroup ) ),
                              () -> assertTrue( actual.contains( overallGroup ) ) );

        // Assert equality for the canonical types: #103804
        Set<GeometryGroup> expectedCanonical = expected.stream()
                                                       .map( FeatureGroup::getGeometryGroup )
                                                       .collect( Collectors.toSet() );

        Set<GeometryGroup> actualCanonical = actual.stream()
                                                   .map( FeatureGroup::getGeometryGroup )
                                                   .collect( Collectors.toSet() );
        assertEquals( expectedCanonical, actualCanonical );

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
        this.testDatabase.createTimeScaleTable( liquibaseDatabase );
        this.testDatabase.createSourceTable( liquibaseDatabase );
        this.testDatabase.createTimeSeriesTable( liquibaseDatabase );
        this.testDatabase.createProjectTable( liquibaseDatabase );
        this.testDatabase.createProjectSourceTable( liquibaseDatabase );
        this.testDatabase.createFeatureTable( liquibaseDatabase );
        this.testDatabase.createEnsembleTable( liquibaseDatabase );
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

    private DatabaseProject getProject( ProjectConfig projectConfig ) throws SQLException
    {
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

        TimeScaleOuter timeScale = TimeScaleOuter.of( Duration.ofMillis( 1 ),
                                                      TimeScaleFunction.UNKNOWN );
        TimeScaleDetails timeScaleDetails = new TimeScaleDetails( timeScale );
        timeScaleDetails.save( this.wresDatabase );
        Long timeScaleId = timeScaleDetails.getId();
        assertNotNull( timeScaleId );

        // Add two "forecasts" with different feature ids
        SourceDetails sourceDetails = new SourceDetails( "abc" );
        sourceDetails.setFeatureId( feature.getId() );
        sourceDetails.setTimeScaleId( timeScaleId );
        sourceDetails.setMeasurementUnitId( measurementUnitId );
        sourceDetails.setVariableName( VARIABLE_NAME );
        sourceDetails.save( this.wresDatabase );
        assertTrue( sourceDetails.performedInsert() );
        Long sourceId = sourceDetails.getId();
        assertNotNull( sourceId );


        SourceDetails sourceDetailsTwo = new SourceDetails( "def" );
        sourceDetailsTwo.setFeatureId( anotherFeature.getId() );
        sourceDetailsTwo.setTimeScaleId( timeScaleId );
        sourceDetailsTwo.setMeasurementUnitId( measurementUnitId );
        sourceDetailsTwo.setVariableName( VARIABLE_NAME );
        sourceDetailsTwo.save( this.wresDatabase );
        assertTrue( sourceDetailsTwo.performedInsert() );
        Long sourceTwoId = sourceDetailsTwo.getId();
        assertNotNull( sourceTwoId );

        EnsembleDetails ensemble = new EnsembleDetails();
        ensemble.setEnsembleName( "ENS123" );
        ensemble.save( this.wresDatabase );
        Long ensembleId = ensemble.getId();


        wres.io.data.details.TimeSeries firstTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     sourceId );
        // Do the save
        firstTraceRow.getTimeSeriesID();

        wres.io.data.details.TimeSeries secondTraceRow =
                new wres.io.data.details.TimeSeries( this.wresDatabase,
                                                     ensembleId,
                                                     sourceTwoId );
        // Do the save
        secondTraceRow.getTimeSeriesID();

        // Add a project
        DatabaseProject project =
                new DatabaseProject( this.wresDatabase,
                                     this.mockCaches,
                                     null,
                                     projectConfig,
                                     PROJECT_HASH );
        boolean saved = project.save();

        assertTrue( saved );

        assertEquals( PROJECT_HASH, project.getHash() );

        // Add the same project source to each side, this should be done in one
        // transaction rather than four, technically. See Projects methods.
        String sourceInsert =
                "INSERT INTO wres.ProjectSource (project_id, source_id, member) VALUES ({0},{1},''{2}'')";

        String rightSourceInsert = MessageFormat.format( sourceInsert,
                                                         project.getId(),
                                                         sourceId,
                                                         LeftOrRightOrBaseline.RIGHT.value() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                rightSourceInsert );
        int rowCount = script.execute();

        assertEquals( 1, rowCount );

        String leftSourceInsert = MessageFormat.format( sourceInsert,
                                                        project.getId(),
                                                        sourceId,
                                                        LeftOrRightOrBaseline.LEFT.value() );

        DataScripter leftScript = new DataScripter( this.wresDatabase,
                                                    leftSourceInsert );
        int rowsLeft = leftScript.execute();
        assertEquals( 1, rowsLeft );

        String rightSourceInsertTwo = MessageFormat.format( sourceInsert,
                                                            project.getId(),
                                                            sourceTwoId,
                                                            LeftOrRightOrBaseline.RIGHT.value() );

        DataScripter rightScriptTwo = new DataScripter( this.wresDatabase,
                                                        rightSourceInsertTwo );
        int rowCountTwo = rightScriptTwo.execute();

        assertEquals( 1, rowCountTwo );

        String leftSourceInsertTwo = MessageFormat.format( sourceInsert,
                                                           project.getId(),
                                                           sourceTwoId,
                                                           LeftOrRightOrBaseline.LEFT.value() );

        DataScripter leftScriptTwo = new DataScripter( this.wresDatabase,
                                                       leftSourceInsertTwo );
        int rowsLeftTwo = leftScriptTwo.execute();
        assertEquals( 1, rowsLeftTwo );

        return project;
    }

    /**
     * @return some project declaration
     */

    private ProjectConfig getProjectConfig()
    {
        String featureName = FEATURE.getName();
        String anotherName = ANOTHER_FEATURE.getName();
        List<NamedFeature> features =
                List.of( new NamedFeature( featureName, featureName, null ), new NamedFeature( anotherName, anotherName, null ) );
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
                                                  null ),
                                  null,
                                  null,
                                  null,
                                  "test_project" );
    }

}
