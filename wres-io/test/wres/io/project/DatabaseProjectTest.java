package wres.io.project;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
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

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureGroups;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.io.database.ConnectionSupplier;
import wres.io.database.caching.DatabaseCaches;
import wres.io.database.caching.Features;
import wres.io.database.details.EnsembleDetails;
import wres.io.database.details.FeatureDetails;
import wres.io.database.details.MeasurementDetails;
import wres.io.database.details.SourceDetails;
import wres.io.database.details.TimeScaleDetails;
import wres.io.database.DataScripter;
import wres.io.database.TestDatabase;
import wres.io.database.details.TimeSeries;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
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
    private static final Feature FEATURE = Feature.of(
            wres.statistics.MessageFactory.getGeometry( "F" ) );
    private static final Feature ANOTHER_FEATURE = Feature.of(
            wres.statistics.MessageFactory.getGeometry( "G" ) );
    private static final String PROJECT_HASH = "881hfEaffja267";
    private static final String UNITS = "[ft_i]3/s";
    private static final String VARIABLE_NAME = "V";

    @Mock
    private SystemSettings mockSystemSettings;

    @Mock
    private ConnectionSupplier mockConnectionSupplier;

    private wres.io.database.Database wresDatabase;

    @Mock
    private DatabaseCaches mockCaches;

    @Mock
    private DatabaseSettings mockDatabaseSettings;

    private TestDatabase testDatabase;
    private HikariDataSource dataSource;
    private Connection rawConnection;
    private DatabaseProject project;
    private Database liquibaseDatabase;

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
        Mockito.when( this.mockSystemSettings.getDatabaseConfiguration() )
               .thenReturn( this.mockDatabaseSettings );
        Mockito.when( this.mockConnectionSupplier.getConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockConnectionSupplier.getSystemSettings() )
               .thenReturn( this.mockSystemSettings );
        Mockito.when( this.mockConnectionSupplier.getHighPriorityConnectionPool() )
               .thenReturn( this.dataSource );
        Mockito.when( this.mockSystemSettings.getDatabaseConfiguration().getDatabaseType() )
               .thenReturn( DatabaseType.H2 );
        Mockito.when( this.mockDatabaseSettings.getMaxPoolSize() )
               .thenReturn( 10 );

        this.wresDatabase = new wres.io.database.Database( this.mockConnectionSupplier );

        // Set up a liquibase database to run migrations against.
        this.liquibaseDatabase = this.testDatabase.createNewLiquibaseDatabase( this.rawConnection );

        Features featuresCache = new Features( this.wresDatabase );

        Mockito.when( this.mockCaches.getFeaturesCache() )
               .thenReturn( featuresCache );

        // Create the tables
        this.addTheDatabaseAndTables();

        // Get a project for testing, backed by data
        EvaluationDeclaration declaration = this.getDeclaration();
        this.project = this.getProject( declaration );
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
    void testGetFeatures()
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
    void testGetFeatureGroups()
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

    @Test
    void testGetEnsembleLabels()
    {
        SortedSet<String> actual = this.project.getEnsembleLabels( DatasetOrientation.RIGHT );
        assertEquals( new TreeSet<>( Set.of( "ENS123" ) ), actual );
    }

    @Test
    void saveProject() throws LiquibaseException
    {
        // Add the project table
        this.testDatabase.createProjectTable( this.liquibaseDatabase );

        Project project = new DatabaseProject( this.wresDatabase,
                                               this.mockCaches,
                                               null,
                                               EvaluationDeclarationBuilder.builder()
                                                                           .build(),
                                               "321" );
        boolean saved = project.save();
        assertTrue( saved, "Expected project details to have performed insert." );
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
     * @param declaration the declaration
     * @return a project for testing, backed by data
     * @throws SQLException if the detailed set-up fails
     */

    private DatabaseProject getProject( EvaluationDeclaration declaration ) throws SQLException
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


        TimeSeries firstTraceRow =
                new TimeSeries( this.wresDatabase,
                                ensembleId,
                                sourceId );
        // Do the save
        firstTraceRow.getTimeSeriesID();

        TimeSeries secondTraceRow =
                new TimeSeries( this.wresDatabase,
                                ensembleId,
                                sourceTwoId );
        // Do the save
        secondTraceRow.getTimeSeriesID();

        // Add a project
        DatabaseProject project =
                new DatabaseProject( this.wresDatabase,
                                     this.mockCaches,
                                     null,
                                     declaration,
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
                                                         DatasetOrientation.RIGHT.name()
                                                                                 .toLowerCase() );

        DataScripter script = new DataScripter( this.wresDatabase,
                                                rightSourceInsert );
        int rowCount = script.execute();

        assertEquals( 1, rowCount );

        String leftSourceInsert = MessageFormat.format( sourceInsert,
                                                        project.getId(),
                                                        sourceId,
                                                        DatasetOrientation.LEFT.name()
                                                                               .toLowerCase() );

        DataScripter leftScript = new DataScripter( this.wresDatabase,
                                                    leftSourceInsert );
        int rowsLeft = leftScript.execute();
        assertEquals( 1, rowsLeft );

        String rightSourceInsertTwo = MessageFormat.format( sourceInsert,
                                                            project.getId(),
                                                            sourceTwoId,
                                                            DatasetOrientation.RIGHT.name()
                                                                                    .toLowerCase() );

        DataScripter rightScriptTwo = new DataScripter( this.wresDatabase,
                                                        rightSourceInsertTwo );
        int rowCountTwo = rightScriptTwo.execute();

        assertEquals( 1, rowCountTwo );

        String leftSourceInsertTwo = MessageFormat.format( sourceInsert,
                                                           project.getId(),
                                                           sourceTwoId,
                                                           DatasetOrientation.LEFT.name()
                                                                                  .toLowerCase() );

        DataScripter leftScriptTwo = new DataScripter( this.wresDatabase,
                                                       leftSourceInsertTwo );
        int rowsLeftTwo = leftScriptTwo.execute();
        assertEquals( 1, rowsLeftTwo );

        return project;
    }

    /**
     * @return some project declaration
     */

    private EvaluationDeclaration getDeclaration()
    {
        String featureName = FEATURE.getName();
        String anotherName = ANOTHER_FEATURE.getName();
        Geometry geometry = Geometry.newBuilder()
                                    .setName( featureName )
                                    .build();
        Geometry anotherGeometry = Geometry.newBuilder()
                                           .setName( anotherName )
                                           .build();
        Set<GeometryTuple> features =
                Set.of( GeometryTuple.newBuilder()
                                     .setLeft( geometry )
                                     .setRight( geometry )
                                     .build(),
                        GeometryTuple.newBuilder()
                                     .setLeft( anotherGeometry )
                                     .setRight( anotherGeometry )
                                     .build() );
        Set<GeometryGroup> featureGroups = Set.of( GeometryGroup.newBuilder()
                                                                .setRegionName( "A feature group!" )
                                                                .addAllGeometryTuples( features )
                                                                .build() );

        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.OBSERVATIONS )
                                     .build();

        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        return EvaluationDeclarationBuilder.builder()
                                           .left( left )
                                           .right( right )
                                           .features( new wres.config.yaml.components.Features( features ) )
                                           .featureGroups( new FeatureGroups( featureGroups ) )
                                           .label( "test_project" )
                                           .build();
    }
}
