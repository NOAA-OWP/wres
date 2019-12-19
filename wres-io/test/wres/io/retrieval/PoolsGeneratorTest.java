package wres.io.retrieval;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DurationUnit;
import wres.config.generated.Feature;
import wres.config.generated.IntBoundsType;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.datamodel.Ensemble;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.PoolFactory;
import wres.io.retrieval.PoolsGenerator;
import wres.io.retrieval.UnitMapper;

/**
 * Tests the {@link PoolsGenerator}.
 * 
 * @author james.brown@hydrosolved.com
 */
@RunWith( PowerMockRunner.class )
@PrepareForTest( { UnitMapper.class, ConfigHelper.class } )
@PowerMockIgnore( { "javax.management.*", "java.io.*", "javax.xml.*", "com.sun.*", "org.xml.*" } )
public class PoolsGeneratorTest
{

    private static final String GET_VARIABLE_ID_FROM_PROJECT_CONFIG = "getVariableIdFromProjectConfig";

    private static final String STREAMFLOW = "STREAMFLOW";

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
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        List<DataSourceConfig.Source> sourceList = new ArrayList<>();

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      sourceList,
                                                      new Variable( "DISCHARGE", null, null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "single valued forecasts" ),
                                                       sourceList,
                                                       new Variable( STREAMFLOW, null, null ),
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

        Feature feature =
                new Feature( null, null, null, null, null, "FAKE2", null, null, null, null, null, null, null );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( 12345 );
        Mockito.when( project.getLeftVariableFeatureId( feature ) ).thenReturn( 1 );
        Mockito.when( project.getRightVariableFeatureId( feature ) ).thenReturn( 2 );
        Mockito.when( project.getBaselineVariableFeatureId( feature ) ).thenReturn( 3 );
        Mockito.when( project.hasBaseline() ).thenReturn( false );
        Mockito.when( project.usesProbabilityThresholds() ).thenReturn( false );

        // Mock the unit mapper
        UnitMapper mapper = Mockito.mock( UnitMapper.class );
        PowerMockito.mockStatic( UnitMapper.class );
        PowerMockito.when( UnitMapper.class, "of", "CFS" )
                    .thenReturn( mapper );

        PowerMockito.mockStatic( ConfigHelper.class );
        PowerMockito.when( ConfigHelper.class, GET_VARIABLE_ID_FROM_PROJECT_CONFIG, projectConfig, false )
                    .thenReturn( STREAMFLOW );
        PowerMockito.when( ConfigHelper.class, GET_VARIABLE_ID_FROM_PROJECT_CONFIG, projectConfig, true )
                    .thenReturn( STREAMFLOW );

        // Create the actual output
        List<Supplier<PoolOfPairs<Double, Double>>> actual =
                PoolFactory.getSingleValuedPools( project, feature, mapper );

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
                                                 leadBoundsConfig,
                                                 null,
                                                 null,
                                                 issuedDatesConfig,
                                                 null,
                                                 null,
                                                 null,
                                                 issuedDatesPoolingWindowConfig,
                                                 leadTimesPoolingWindowConfig,
                                                 null,
                                                 null );
        List<DataSourceConfig.Source> sourceList = new ArrayList<>();

        DataSourceConfig left = new DataSourceConfig( DatasourceType.fromValue( "observations" ),
                                                      sourceList,
                                                      new Variable( "DISCHARGE", null, null ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( DatasourceType.fromValue( "ensemble forecasts" ),
                                                       sourceList,
                                                       new Variable( STREAMFLOW, null, null ),
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

        Feature feature =
                new Feature( null, null, null, null, null, "FAKE2", null, null, null, null, null, null, null );

        // Mock the sufficient elements of Project
        Project project = Mockito.mock( Project.class );
        Mockito.when( project.getProjectConfig() ).thenReturn( projectConfig );
        Mockito.when( project.getId() ).thenReturn( 12345 );
        Mockito.when( project.getLeftVariableFeatureId( feature ) ).thenReturn( 1 );
        Mockito.when( project.getRightVariableFeatureId( feature ) ).thenReturn( 2 );
        Mockito.when( project.getBaselineVariableFeatureId( feature ) ).thenReturn( 3 );
        Mockito.when( project.hasBaseline() ).thenReturn( false );
        Mockito.when( project.usesProbabilityThresholds() ).thenReturn( false );

        // Mock the unit mapper
        UnitMapper mapper = Mockito.mock( UnitMapper.class );
        PowerMockito.mockStatic( UnitMapper.class );
        PowerMockito.when( UnitMapper.class, "of", "CFS" )
                    .thenReturn( mapper );

        PowerMockito.mockStatic( ConfigHelper.class );
        PowerMockito.when( ConfigHelper.class, GET_VARIABLE_ID_FROM_PROJECT_CONFIG, projectConfig, false )
                    .thenReturn( STREAMFLOW );
        PowerMockito.when( ConfigHelper.class, GET_VARIABLE_ID_FROM_PROJECT_CONFIG, projectConfig, true )
                    .thenReturn( STREAMFLOW );

        // Create the actual output
        List<Supplier<PoolOfPairs<Double, Ensemble>>> actual = PoolFactory.getEnsemblePools( project, feature, mapper );

        // Assert expected number of suppliers
        assertEquals( 18, actual.size() );
    }


}
