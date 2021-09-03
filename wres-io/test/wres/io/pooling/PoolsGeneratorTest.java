package wres.io.pooling;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
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
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.project.Project;
import wres.io.retrieval.EnsembleRetrieverFactory;
import wres.io.retrieval.RetrieverFactory;
import wres.io.retrieval.SingleValuedRetrieverFactory;
import wres.io.retrieval.UnitMapper;
import wres.io.utilities.Database;
import wres.statistics.generated.Evaluation;

/**
 * Tests the {@link PoolsGenerator}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class PoolsGeneratorTest
{

    private static final String STREAMFLOW = "STREAMFLOW";

    private @Mock Database wresDatabase;
    private @Mock Features featuresCache;
    private @Mock Ensembles ensemblesCache;
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
        PairConfig pairsConfig = new PairConfig( "CFS",
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

        FeatureKey feature = FeatureKey.of( "FAKE2" );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( 12345L );
        Mockito.when( project.getLeftVariableName() ).thenReturn( "DISCHARGE" );
        Mockito.when( project.getRightVariableName() ).thenReturn( STREAMFLOW );
        Mockito.when( project.getBaselineVariableName() ).thenReturn( null );
        Mockito.when( project.hasBaseline() ).thenReturn( false );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );
        Mockito.when( project.getDatabase() ).thenReturn( this.wresDatabase );
        Mockito.when( project.getFeaturesCache() ).thenReturn( this.featuresCache );

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

        FeatureGroup featureGroup = FeatureGroup.of( new FeatureTuple( feature, feature, null ) );

        // Create the actual output
        List<Supplier<Pool<Pair<Double, Double>>>> actual =
                PoolFactory.getSingleValuedPools( evaluationDescription,
                                                  project,
                                                  featureGroup,
                                                  this.unitMapper,
                                                  retrieverFactory );

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
        PairConfig pairsConfig = new PairConfig( "CFS",
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

        FeatureKey feature = FeatureKey.of( "FAKE2" );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( 12345L );
        Mockito.when( project.getLeftVariableName() ).thenReturn( "DISCHARGE" );
        Mockito.when( project.getRightVariableName() ).thenReturn( STREAMFLOW );
        Mockito.when( project.getBaselineVariableName() ).thenReturn( null );
        Mockito.when( project.hasBaseline() ).thenReturn( false );
        Mockito.when( project.hasProbabilityThresholds() ).thenReturn( false );
        Mockito.when( project.getDatabase() ).thenReturn( this.wresDatabase );
        Mockito.when( project.getFeaturesCache() ).thenReturn( this.featuresCache );
        Mockito.when( project.getEnsemblesCache() ).thenReturn( this.ensemblesCache );

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

        FeatureGroup featureGroup = FeatureGroup.of( new FeatureTuple( feature, feature, null ) );

        // Create the actual output
        List<Supplier<Pool<Pair<Double, Ensemble>>>> actual =
                PoolFactory.getEnsemblePools( evaluationDescription,
                                              project,
                                              featureGroup,
                                              this.unitMapper,
                                              retrieverFactory );

        // Assert expected number of suppliers
        assertEquals( 18, actual.size() );
    }


}
