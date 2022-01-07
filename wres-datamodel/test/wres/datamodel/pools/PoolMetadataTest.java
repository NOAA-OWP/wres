package wres.datamodel.pools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;

import wres.config.ProjectConfigPlus;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;

/**
 * Tests the {@link PoolMetadata}.
 * 
 * @author James Brown
 */

public class PoolMetadataTest
{

    private static final String HEFS = "HEFS";
    private static final String SQIN = "SQIN";
    private static final String TEST_DIMENSION = "SOME_DIM";
    private static final String DRRC3 = "DRRC3";
    private static final String DRRC2 = "DRRC2";
    private static final String THIRD_TIME = "2000-02-02T00:00:00Z";
    private static final String SECOND_TIME = "1986-01-01T00:00:00Z";
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    /**
     * Tests construction of the {@link PoolMetadata} using the various construction options.
     */

    @Test
    public void testBuildProducesNonNullInstances()
    {
        assertNotNull( PoolMetadata.of() );

        assertNotNull( PoolMetadata.of( Evaluation.newBuilder()
                                                  .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                  .build(),
                                        Pool.getDefaultInstance() ) );
        FeatureKey a = FeatureKey.of( MessageFactory.getGeometry( "A" ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "B" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool poolOne = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( a, a, a ) ),
                                             null,
                                             null,
                                             null,
                                             false,
                                             1 );

        assertNotNull( PoolMetadata.of( evaluation, poolOne ) );

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                  Operator.EQUAL,
                                                                                  ThresholdDataType.LEFT ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), thresholds ) );

        TimeWindowOuter timeWindow =
                TimeWindowOuter.of( Instant.parse( THIRD_TIME ), Instant.parse( THIRD_TIME ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), timeWindow ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), timeWindow, thresholds ) );

        Pool pool = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( a, a, a ) ),
                                          timeWindow,
                                          TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                             TimeScaleFunction.MEAN ),
                                          thresholds,
                                          false,
                                          1 );

        assertNotNull( PoolMetadata.of( evaluation, pool ) );
    }

    /**
     * Test {@link PoolMetadata#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertEquals( PoolMetadata.of(), PoolMetadata.of() );
        FeatureKey l1 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l1, l1, l1 ) ),
                                          null,
                                          null,
                                          null,
                                          false,
                                          1 );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );

        // Reflexive
        assertEquals( m1, m1 );
        FeatureKey l2 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l2, l2, l2 ) ), null, null, null, false, 1 );

        PoolMetadata m2 = PoolMetadata.of( evaluationTwo, poolTwo );

        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );
        FeatureKey l3 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( SQIN )
                                               .setRightDataName( HEFS )
                                               .setMeasurementUnit( TEST_DIMENSION )
                                               .build();

        Pool poolThree =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l3, l3, l3 ) ), null, null, null, false, 1 );

        PoolMetadata m3 = PoolMetadata.of( evaluationThree, poolThree );

        FeatureKey l4 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluationFour = Evaluation.newBuilder()
                                              .setRightVariableName( SQIN )
                                              .setRightDataName( HEFS )
                                              .setMeasurementUnit( TEST_DIMENSION )
                                              .build();

        Pool poolFour =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l4, l4, l4 ) ), null, null, null, false, 1 );

        PoolMetadata m4 = PoolMetadata.of( evaluationFour, poolFour );
        assertEquals( m3, m4 );
        assertNotEquals( m1, m3 );

        // Transitive
        FeatureKey l4t = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluationFive = Evaluation.newBuilder()
                                              .setRightVariableName( SQIN )
                                              .setRightDataName( HEFS )
                                              .setMeasurementUnit( TEST_DIMENSION )
                                              .build();

        Pool poolFive =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l4t, l4t, l4t ) ),
                                      null,
                                      null,
                                      null,
                                      false,
                                      1 );

        PoolMetadata m4t = PoolMetadata.of( evaluationFive, poolFive );
        assertEquals( m4, m4t );
        assertEquals( m3, m4t );

        // Unequal
        FeatureKey l5 = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );
        Pool poolSix =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l5, l5, l5 ) ), null, null, null, false, 1 );
        PoolMetadata m5 = PoolMetadata.of( evaluationFive, poolSix );
        assertNotEquals( m4, m5 );
        PoolMetadata m5NoDim = PoolMetadata.of( Evaluation.newBuilder()
                                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                          .build(),
                                                poolSix );
        assertNotEquals( m5, m5NoDim );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( m1.equals( m2 ) );
        }
        // Add a time window
        TimeWindowOuter firstWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        FeatureKey l6 = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );

        Pool poolSeven = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l6, l6, l6 ) ),
                                               firstWindow,
                                               null,
                                               null,
                                               false,
                                               1 );

        PoolMetadata m6 = PoolMetadata.of( evaluationFive, poolSeven );

        FeatureKey l7 = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );

        Pool poolEight = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l7, l7, l7 ) ),
                                               secondWindow,
                                               null,
                                               null,
                                               false,
                                               1 );

        PoolMetadata m7 = PoolMetadata.of( evaluationFive, poolEight );

        assertEquals( m6, m7 );
        assertEquals( m7, m6 );
        assertNotEquals( m3, m6 );

        TimeWindowOuter thirdWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ),
                                                          Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        FeatureKey l8 = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );

        Pool poolNine = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                              thirdWindow,
                                              null,
                                              null,
                                              false,
                                              1 );

        PoolMetadata m8 = PoolMetadata.of( evaluationFive, poolNine );

        assertNotEquals( m6, m8 );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Pool poolTen = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                             thirdWindow,
                                             null,
                                             thresholds,
                                             false,
                                             1 );

        PoolMetadata m9 = PoolMetadata.of( evaluationFive, poolTen );

        assertNotEquals( m8, m9 );

        PoolMetadata m10 = PoolMetadata.of( evaluationFive, poolTen );

        assertEquals( m9, m10 );

        // Add a project configuration
        ProjectConfig mockConfigOne =
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
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   new PairConfig( "CMS",
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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
                                                                     0,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      MetricConfigName.BIAS_FRACTION ) ),
                                                                     null ) ),
                                   null,
                                   null,
                                   null );

        ProjectConfig mockConfigTwo =
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
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   new PairConfig( "CMS",
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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
                                                                     0,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      MetricConfigName.BIAS_FRACTION ) ),
                                                                     null ) ),
                                   null,
                                   null,
                                   null );
        TimeWindowOuter timeWindow = thirdWindow;
        OneOrTwoThresholds thresholds1 = thresholds;

        ProjectConfigPlus mockConfigOnePlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockConfigOnePlus.getProjectConfig() )
               .thenReturn( mockConfigOne );

        Evaluation evaluationSix = MessageFactory.parse( mockConfigOnePlus );

        Pool poolEleven = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                                timeWindow,
                                                null,
                                                thresholds1,
                                                false,
                                                1 );

        PoolMetadata m11 = PoolMetadata.of( evaluationSix, poolEleven );

        TimeWindowOuter timeWindow1 = thirdWindow;
        OneOrTwoThresholds thresholds2 = thresholds;

        ProjectConfigPlus mockConfigTwoPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockConfigTwoPlus.getProjectConfig() )
               .thenReturn( mockConfigTwo );

        Evaluation evaluationSeven = MessageFactory.parse( mockConfigTwoPlus );

        Pool poolTwelve = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                                timeWindow1,
                                                null,
                                                thresholds2,
                                                false,
                                                1 );

        PoolMetadata m12 = PoolMetadata.of( evaluationSeven, poolTwelve );

        assertEquals( m11, m12 );

        // Add a time scale
        Pool poolThirteen = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                                  timeWindow1,
                                                  TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ),
                                                  thresholds2,
                                                  false,
                                                  1 );


        PoolMetadata m13 = PoolMetadata.of( evaluationSeven, poolThirteen );

        PoolMetadata m14 = PoolMetadata.of( evaluationSeven, poolThirteen );

        Pool poolFourteen = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                                  timeWindow1,
                                                  TimeScaleOuter.of( Duration.ofDays( 2 ), TimeScaleFunction.MEAN ),
                                                  thresholds2,
                                                  false,
                                                  1 );


        PoolMetadata m15 = PoolMetadata.of( evaluationSeven, poolFourteen );

        assertEquals( m13, m14 );

        assertNotEquals( m13, m15 );

        // Null check
        assertNotEquals( null, m6 );

        // Other type check
        assertNotEquals( m6, Double.valueOf( 2 ) );
    }

    /**
     * Test {@link PoolMetadata#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertEquals( PoolMetadata.of().hashCode(), PoolMetadata.of().hashCode() );
        FeatureKey l1 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l1, l1, l1 ) ),
                                          null,
                                          null,
                                          null,
                                          false,
                                          1 );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );
        assertEquals( m1.hashCode(), m1.hashCode() );
        FeatureKey l2 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l2, l2, l2 ) ), null, null, null, false, 1 );

        PoolMetadata m2 = PoolMetadata.of( evaluationTwo, poolTwo );
        assertEquals( m1.hashCode(), m2.hashCode() );
        FeatureKey l3 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( SQIN )
                                               .setRightDataName( HEFS )
                                               .setMeasurementUnit( PoolMetadataTest.TEST_DIMENSION )
                                               .build();

        Pool poolThree =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l3, l3, l3 ) ), null, null, null, false, 1 );

        PoolMetadata m3 = PoolMetadata.of( evaluationThree, poolThree );
        FeatureKey l4 = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Pool poolFour =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l4, l4, l4 ) ), null, null, null, false, 1 );

        PoolMetadata m4 = PoolMetadata.of( evaluationThree, poolFour );
        assertEquals( m3.hashCode(), m4.hashCode() );
        FeatureKey l4t = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );

        Pool poolFive =
                MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l4t, l4t, l4t ) ),
                                      null,
                                      null,
                                      null,
                                      false,
                                      1 );

        PoolMetadata m4t = PoolMetadata.of( evaluationThree, poolFive );
        assertEquals( m4.hashCode(), m4t.hashCode() );
        assertEquals( m3.hashCode(), m4t.hashCode() );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( m1.hashCode() == m2.hashCode() );
        }

        // Add a time window
        TimeWindowOuter firstWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        FeatureKey l6 = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );

        Pool poolSix = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l6, l6, l6 ) ),
                                             firstWindow,
                                             null,
                                             null,
                                             false,
                                             1 );

        PoolMetadata m6 = PoolMetadata.of( evaluationThree, poolSix );

        FeatureKey l7 = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );

        Pool poolSeven = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l7, l7, l7 ) ),
                                               secondWindow,
                                               null,
                                               null,
                                               false,
                                               1 );

        PoolMetadata m7 = PoolMetadata.of( evaluationThree, poolSeven );

        assertEquals( m6.hashCode(), m7.hashCode() );
        assertEquals( m7.hashCode(), m6.hashCode() );

        TimeWindowOuter thirdWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ),
                                                          Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        FeatureKey l8 = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Pool poolEight = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                               thirdWindow,
                                               null,
                                               thresholds,
                                               false,
                                               1 );

        PoolMetadata m9 = PoolMetadata.of( evaluationThree, poolEight );

        PoolMetadata m10 = PoolMetadata.of( evaluationThree, poolEight );

        assertEquals( m9.hashCode(), m10.hashCode() );

        // Add a project configuration
        ProjectConfig mockConfigOne =
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
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   new PairConfig( "CMS",
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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
                                                                     0,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      MetricConfigName.BIAS_FRACTION ) ),
                                                                     null ) ),
                                   null,
                                   null,
                                   null );

        ProjectConfig mockConfigTwo =
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
                                                                     null,
                                                                     null,
                                                                     null ),
                                               null ),
                                   new PairConfig( "CMS",
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
                                                   null,
                                                   null,
                                                   null,
                                                   null,
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
                                                                     0,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      MetricConfigName.BIAS_FRACTION ) ),
                                                                     null ) ),
                                   null,
                                   null,
                                   null );
        TimeWindowOuter timeWindow = thirdWindow;
        OneOrTwoThresholds thresholds1 = thresholds;

        ProjectConfigPlus mockConfigOnePlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockConfigOnePlus.getProjectConfig() )
               .thenReturn( mockConfigOne );

        Evaluation evaluationFour = MessageFactory.parse( mockConfigOnePlus );

        Pool poolNine = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                              timeWindow,
                                              null,
                                              thresholds1,
                                              false,
                                              1 );

        PoolMetadata m11 = PoolMetadata.of( evaluationFour, poolNine );

        ProjectConfigPlus mockConfigTwoPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockConfigTwoPlus.getProjectConfig() )
               .thenReturn( mockConfigTwo );

        Evaluation evaluationFive = MessageFactory.parse( mockConfigTwoPlus );

        Pool poolTen = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( l8, l8, l8 ) ),
                                             thirdWindow,
                                             null,
                                             thresholds,
                                             false,
                                             1 );

        PoolMetadata m12 = PoolMetadata.of( evaluationFive, poolTen );

        assertEquals( m11.hashCode(), m12.hashCode() );
    }

    @Test
    public void testGetMetadata()
    {
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setMeasurementUnit( SQIN )
                                          .build();

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA );
        TimeWindowOuter timeWindow = TimeWindowOuter.of();

        FeatureKey left = FeatureKey.of( MessageFactory.getGeometry( DRRC2 ) );
        FeatureKey right = FeatureKey.of( MessageFactory.getGeometry( DRRC3 ) );

        Pool pool = MessageFactory.getPool( FeatureGroup.of( new FeatureTuple( left, right, null ) ),
                                          timeWindow,
                                          TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                             TimeScaleFunction.MEAN ),
                                          thresholds,
                                          false,
                                          1 );

        PoolMetadata metadata = PoolMetadata.of( evaluation, pool );

        assertEquals( Set.of( new FeatureTuple( left, right, null ) ), metadata.getFeatureTuples() );
    }

}
