package wres.io.pooling;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import wres.config.ProjectConfigPlus;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DurationUnit;
import wres.config.generated.IntBoundsType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeSeries;
import wres.io.data.caching.DatabaseCaches;
import wres.io.project.Project;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.database.EnsembleRetrieverFactory;
import wres.io.retrieval.database.SingleValuedRetrieverFactory;
import wres.io.utilities.Database;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link PoolsGenerator}.
 * 
 * @author James Brown
 */
public class PoolsGeneratorTest
{
    private static final String CFS = "CFS";
    private static final String STREAMFLOW = "STREAMFLOW";

    private @Mock Database wresDatabase;
    private @Mock DatabaseCaches caches;
    private @Mock UnitMapper unitMapper;

    @Before
    public void setup()
    {
        MockitoAnnotations.openMocks( this );
    }

    /**
     * Tests {@link PoolsGenerator#get()} using project declaration that is representative of system test
     * scenario505 as of commit 43332ccbb45e712722ef2ca52904b18d8f98397c.
     * @throws Exception if the test set-up fails
     */

    @Test
    public void testGetProducesEighteenPoolSuppliersForSingleValuedCase() throws Exception
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 40 );
        // (2551-03-17T00:00:00Z, 2551-03-20T00:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( "2551-03-17T00:00:00Z", "2551-03-20T00:00:00Z" );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 23, 17, DurationUnit.HOURS );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( CFS,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        List<DataSourceConfig.Source> sourceList = new ArrayList<>();

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      sourceList,
                                                      new Variable( "DISCHARGE", null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                       sourceList,
                                                       new Variable( STREAMFLOW, null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      right,
                                                                      null );

        ProjectConfig projectConfig = new ProjectConfig( inputsConfig, pairsConfig, null, null, null, null );

        Geometry feature = MessageFactory.getGeometry( "FAKE2" );
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( feature, feature, null );
        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( 12345L );
        Mockito.when( project.getVariableName( LeftOrRightOrBaseline.LEFT ) ).thenReturn( "DISCHARGE" );
        Mockito.when( project.getVariableName( LeftOrRightOrBaseline.RIGHT ) ).thenReturn( STREAMFLOW );
        Mockito.when( project.getVariableName( LeftOrRightOrBaseline.BASELINE ) ).thenReturn( null );
        Mockito.when( project.hasBaseline() ).thenReturn( false );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );
        Mockito.when( project.getFeatureGroups() ).thenReturn( Set.of( featureGroup ) );
        Mockito.when( project.getMeasurementUnit() ).thenReturn( CFS );

        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() )
               .thenReturn( projectConfig );

        Evaluation evaluationDescription = MessageFactory.parse( projectConfigPlus );

        // Mock a feature-shaped retriever factory
        RetrieverFactory<Double, Double> retrieverFactory = Mockito.mock( SingleValuedRetrieverFactory.class );
        Mockito.when( retrieverFactory.getLeftRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( () -> Stream.of() );
        Mockito.when( retrieverFactory.getRightRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( () -> Stream.of() );

        PoolFactory poolFactory = PoolFactory.of( project );

        PoolParameters poolParameters = new PoolParameters.Builder().build();
        List<PoolRequest> poolRequests = poolFactory.getPoolRequests( evaluationDescription );

        // Create the actual output
        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>>> actual =
                poolFactory.getSingleValuedPools( poolRequests,
                                                  retrieverFactory,
                                                  poolParameters );

        // Assert expected number of suppliers
        assertEquals( 18, actual.size() );
    }

    /**
     * Tests {@link PoolsGenerator#get()} using project declaration that is representative of system test
     * scenario505 as of commit 43332ccbb45e712722ef2ca52904b18d8f98397c. While that scenario does not supply ensemble 
     * data, the purpose of this test is to assert that the correct number of pools is generated, rather than the 
     * contents of each pool.
     * @throws Exception if the test set-up fails
     */

    @Test
    public void testGetProducesEighteenPoolSuppliersForEnsembleCase() throws Exception
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 40 );
        // (2551-03-17T00:00:00Z, 2551-03-20T00:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( "2551-03-17T00:00:00Z", "2551-03-20T00:00:00Z" );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 23, 17, DurationUnit.HOURS );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( CFS,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 null,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        List<DataSourceConfig.Source> sourceList = new ArrayList<>();

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      sourceList,
                                                      new Variable( "DISCHARGE", null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "ensemble forecasts" ),
                                                       sourceList,
                                                       new Variable( STREAMFLOW, null ),
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       null );

        ProjectConfig.Inputs inputsConfig = new ProjectConfig.Inputs( left,
                                                                      right,
                                                                      null );

        ProjectConfig projectConfig = new ProjectConfig( inputsConfig, pairsConfig, null, null, null, null );

        Geometry feature = MessageFactory.getGeometry( "FAKE2" );
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( feature, feature, null );
        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( null, geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( 12345L );
        Mockito.when( project.getVariableName( LeftOrRightOrBaseline.LEFT ) ).thenReturn( "DISCHARGE" );
        Mockito.when( project.getVariableName( LeftOrRightOrBaseline.RIGHT ) ).thenReturn( STREAMFLOW );
        Mockito.when( project.getVariableName( LeftOrRightOrBaseline.BASELINE ) ).thenReturn( null );
        Mockito.when( project.hasBaseline() ).thenReturn( false );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );
        Mockito.when( project.getFeatureGroups() ).thenReturn( Set.of( featureGroup ) );
        Mockito.when( project.getMeasurementUnit() ).thenReturn( CFS );

        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() )
               .thenReturn( projectConfig );

        Evaluation evaluationDescription = MessageFactory.parse( projectConfigPlus );

        // Mock a feature-shaped retriever factory
        RetrieverFactory<Double, Ensemble> retrieverFactory = Mockito.mock( EnsembleRetrieverFactory.class );
        Mockito.when( retrieverFactory.getLeftRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( () -> Stream.of() );
        Mockito.when( retrieverFactory.getRightRetriever( Mockito.any(), Mockito.any() ) )
               .thenReturn( () -> Stream.of() );

        PoolFactory poolFactory = PoolFactory.of( project );

        List<PoolRequest> poolRequests = poolFactory.getPoolRequests( evaluationDescription );

        // Create the actual output
        PoolParameters poolParameters = new PoolParameters.Builder().build();
        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Ensemble>>>>>> actual =
                poolFactory.getEnsemblePools( poolRequests,
                                              retrieverFactory,
                                              poolParameters );

        // Assert expected number of suppliers
        assertEquals( 18, actual.size() );
    }


}
