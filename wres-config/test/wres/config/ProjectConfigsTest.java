package wres.config;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;

public class ProjectConfigsTest
{
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link ProjectConfigs#compare(wres.config.generated.ProjectConfig, wres.config.generated.ProjectConfig)}.
     */

    @Test
    public void testCompare()
    {

        // Mock some instances of the project configuration
        // that differ on name
        
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.BIAS_FRACTION ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.VOLUMETRIC_EFFICIENCY ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.SUM_OF_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.ROOT_MEAN_SQUARE_ERROR ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ) );
        metrics.add( new MetricConfig( null, null, MetricConfigName.COEFFICIENT_OF_DETERMINATION ) );

        ProjectConfig mockOne =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   "MockOne" );

        ProjectConfig mockTwo =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   "MockOne" );

        ProjectConfig mockThree =
                new ProjectConfig( new Inputs( null,
                                               new DataSourceConfig( DatasourceType.SINGLE_VALUED_FORECASTS,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null, metrics, null ) ),
                                   null,
                                   null,
                                   "MockThree" );

        // (x.compareTo(y)==0) == (x.equals(y))
        assertTrue( ( ProjectConfigs.compare( mockOne, mockOne ) == 0 ) == mockOne.equals( mockOne ) );

        assertTrue( ( ProjectConfigs.compare( mockOne, mockTwo ) == 0 ) == mockOne.equals( mockTwo ) );

        assertTrue( ( ProjectConfigs.compare( mockOne, mockThree ) == 0 ) == mockOne.equals( mockThree ) );

        assertTrue( mockOne.equals( mockTwo ) && ProjectConfigs.compare( mockOne, mockTwo ) == 0 );

        // sgn(x.compareTo(y)) == -sgn(y.compareTo(x))
        assertTrue( ProjectConfigs.compare( mockOne, mockOne ) == -ProjectConfigs.compare( mockOne, mockOne ) );

        assertTrue( ProjectConfigs.compare( mockOne, mockTwo ) == -ProjectConfigs.compare( mockTwo, mockOne ) );

        assertTrue( ProjectConfigs.compare( mockOne, mockThree ) == -ProjectConfigs.compare( mockThree, mockOne ) );

        // (x.compareTo(y)>0 && y.compareTo(z)>0) implies x.compareTo(z)>0
        assertTrue( ProjectConfigs.compare( mockThree, mockOne ) > 0 && ProjectConfigs.compare( mockThree, mockTwo ) > 0
                    && ProjectConfigs.compare( mockThree, mockTwo ) > 0 );

        // x.compareTo(y)==0 implies that sgn(x.compareTo(z)) == sgn(y.compareTo(z)), for all z.
        assertTrue( ProjectConfigs.compare( mockOne, mockTwo ) == 0
                    && ProjectConfigs.compare( mockOne, mockThree ) == ProjectConfigs.compare( mockTwo, mockThree ) );

        // Order in a container
        List<ProjectConfig> listOfConfigs = Arrays.asList( mockThree, mockTwo, mockOne );
        Collections.sort( listOfConfigs, Comparator.naturalOrder() );

        assertTrue( listOfConfigs.get( 0 ).equals( mockOne ) && listOfConfigs.get( 1 ).equals( mockTwo )
                    && listOfConfigs.get( 2 ).equals( mockThree ) );
        
    }
    
    /**
     * Tests for an expected exception from {@link ProjectConfigs#compare(wres.config.generated.ProjectConfig, 
     * wres.config.generated.ProjectConfig)} when the first input is null.
     */

    @Test
    public void testCompareThrowsNullWhenFirstInputIsNull()
    {
        // Nullity with null input
        exception.expect( NullPointerException.class );        
        exception.expectMessage( "The first input is null, which is not allowed." );
        
        ProjectConfigs.compare( null, new ProjectConfig( null, null, null, null, null, null ) );        
    }
    
    /**
     * Tests for an expected exception from {@link ProjectConfigs#compare(wres.config.generated.ProjectConfig, 
     * wres.config.generated.ProjectConfig)} when the second input is null.
     */

    @Test
    public void testCompareThrowsNullWhenSecondInputIsNull()
    {
        // Nullity with null input
        exception.expect( NullPointerException.class );        
        exception.expectMessage( "The second input is null, which is not allowed." );
        
        ProjectConfigs.compare( new ProjectConfig( null, null, null, null, null, null ), null );        
    }

}
