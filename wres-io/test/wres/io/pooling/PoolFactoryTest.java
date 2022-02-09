package wres.io.pooling;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import wres.config.ProjectConfigPlus;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DurationUnit;
import wres.config.generated.IntBoundsType;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.time.TimeSeries;
import wres.io.project.Project;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.SingleValuedRetrieverFactory;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;

/**
 * Tests the {@link PoolFactory}.
 * 
 * @author James Brown
 */
class PoolFactoryTest
{
    @Test
    public void testGetPoolRequestsForEighteenTimeWindowsAndOneFeatureGroupProducesEighteenPoolRequests()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 0, 40 );
        // (2551-03-17T00:00:00Z, 2551-03-20T00:00:00Z)
        DateCondition issuedDatesConfig = new DateCondition( "2551-03-17T00:00:00Z", "2551-03-20T00:00:00Z" );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 23, 17, DurationUnit.HOURS );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( "CFS",
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
                                                       new Variable( "STREAMFLOW", null ),
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

        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() )
               .thenReturn( projectConfig );

        Evaluation evaluationDescription = MessageFactory.parse( projectConfigPlus );

        Geometry keyOne = MessageFactory.getGeometry( "DRRC2", null, null, null );
        Geometry keyTwo = MessageFactory.getGeometry( "DRRC2HSF", null, null, null );
        Geometry keyThree = MessageFactory.getGeometry( "DRRC2HSF", null, null, null );
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( keyOne, keyTwo, keyThree );
        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( null, geoTuple );

        FeatureGroup groupOne = FeatureGroup.of( geoGroup );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne ) );
        Mockito.when( project.getProjectConfig() )
               .thenReturn( projectConfig );
        List<PoolRequest> actual = PoolFactory.getPoolRequests( evaluationDescription, project );

        assertEquals( 18, actual.size() );
    }


    /**
     * Asserts against the behavior in #101246.
     */

    @Test
    public void testGetSingleValuedPoolsProducesFourtyEightPoolSuppliers()
    {
        // Mock the sufficient elements of the ProjectConfig
        IntBoundsType leadBoundsConfig = new IntBoundsType( 1, 24 );
        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 0, 1, DurationUnit.HOURS );
        PairConfig pairsConfig = new PairConfig( "CFS",
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
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
                                                       new Variable( "STREAMFLOW", null ),
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

        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() )
               .thenReturn( projectConfig );

        Evaluation evaluationDescription = MessageFactory.parse( projectConfigPlus );

        Geometry keyOne = MessageFactory.getGeometry( "DRRC2HSF", null, null, null );
        Geometry keyTwo = MessageFactory.getGeometry( "DRRC2HSF", null, null, null );
        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( keyOne, keyTwo, null );
        GeometryGroup geoGroup = MessageFactory.getGeometryGroup( null, geoTuple );
        GeometryGroup geoGroupTwo = MessageFactory.getGeometryGroup( "aGroup", geoTuple );

        FeatureGroup groupOne = FeatureGroup.of( geoGroup );
        FeatureGroup groupTwo = FeatureGroup.of( geoGroupTwo );

        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getFeatureGroups() )
               .thenReturn( Set.of( groupOne, groupTwo ) );
        Mockito.when( project.getProjectConfig() )
               .thenReturn( projectConfig );
        List<PoolRequest> actual = PoolFactory.getPoolRequests( evaluationDescription, project );

        assertEquals( 48, actual.size() );

        PoolParameters poolParameters = new PoolParameters.Builder().build();
        RetrieverFactory<Double, Double> retrieverFactory = Mockito.mock( SingleValuedRetrieverFactory.class );
        Mockito.when( retrieverFactory.getLeftRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( () -> Stream.of( TimeSeries.of( null ) ) );
        Mockito.when( retrieverFactory.getRightRetriever( Mockito.anySet(), Mockito.any() ) )
               .thenReturn( () -> Stream.of( TimeSeries.of( null ) ) );

        List<Pair<PoolRequest, Supplier<Pool<TimeSeries<Pair<Double, Double>>>>>> suppliers =
                PoolFactory.getSingleValuedPools( project, actual, retrieverFactory, poolParameters );

        // Assert two feature group names
        Set<FeatureGroup> actualFeatureGroups = suppliers.stream()
                                                         .map( next -> next.getLeft().getMetadata().getFeatureGroup() )
                                                         .collect( Collectors.toSet() );

        Set<FeatureGroup> expectedFeatureGroups = Set.of( groupOne, groupTwo );
        assertEquals( expectedFeatureGroups, actualFeatureGroups );

        // Assert the expected number of unique pool identifiers. The actual identifiers will vary since there is one 
        // sequence per class loader and other tests share the class loader
        Set<Long> actualPoolIds = suppliers.stream()
                                           .map( next -> next.getLeft().getMetadata().getPool().getPoolId() )
                                           .collect( Collectors.toSet() );

        assertEquals( 48, actualPoolIds.size() );
    }
}
