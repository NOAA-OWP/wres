package wres.io.pooling;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.statistics.generated.Evaluation;

/**
 * Tests the {@link PoolFactory}.
 * 
 * @author James Brown
 */
class PoolFactoryTest
{
    @Test
    public void testGetPoolRequestsForEighteenTimeWindowsAndTwoFeatureGroupsProducesThirtySixPoolRequests()
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

        FeatureKey keyOne = new FeatureKey( "DRRC2", null, null, null );
        FeatureKey keyTwo = new FeatureKey( "DRRC2HSF", null, null, null );
        FeatureKey keyThree = new FeatureKey( "DRRC2HSF", null, null, null );
        FeatureTuple aTuple = new FeatureTuple( keyOne, keyTwo, keyThree );
        
        FeatureKey keyFour = new FeatureKey( "DOSC1", null, null, null );
        FeatureKey keyFive = new FeatureKey( "DOSC1HSF", null, null, null );
        FeatureKey keySix = new FeatureKey( "DOSC1HSF", null, null, null );
        FeatureTuple anotherTuple = new FeatureTuple( keyFour, keyFive, keySix );
        
        FeatureGroup groupOne = FeatureGroup.of( aTuple );
        FeatureGroup groupTwo = FeatureGroup.of( anotherTuple );
        Set<FeatureGroup> featureGroups = Set.of( groupOne, groupTwo );
        
        List<PoolRequest> actual = PoolFactory.getPoolRequests( evaluationDescription, projectConfig, featureGroups );

        assertEquals( 36, actual.size() );
    }
}
