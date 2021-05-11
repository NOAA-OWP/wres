package wres.datamodel.pools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

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
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
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
 * @author james.brown@hydrosolved.com
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
     * Tests the {@link PoolMetadata#unionOf(java.util.List)} against a benchmark.
     */
    @Test
    public void unionOf()
    {
        FeatureKey l1 = FeatureKey.of( DRRC2 );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool poolOne = MessageFactory.parse( new FeatureTuple( l1, l1, l1 ),
                                             TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                                 Instant.parse( "1985-12-31T23:59:59Z" ) ),
                                             null,
                                             null,
                                             false );

        PoolMetadata m1 = PoolMetadata.of( evaluation, poolOne );

        FeatureKey l2 = FeatureKey.of( DRRC2 );

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ),
                                             TimeWindowOuter.of( Instant.parse( SECOND_TIME ),
                                                                 Instant.parse( "1986-12-31T23:59:59Z" ) ),
                                             null,
                                             null,
                                             false );

        PoolMetadata m2 = PoolMetadata.of( evaluation, poolTwo );

        FeatureKey l3 = FeatureKey.of( DRRC2 );

        Pool poolThree = MessageFactory.parse( new FeatureTuple( l3, l3, l3 ),
                                               TimeWindowOuter.of( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                   Instant.parse( "1988-01-01T00:00:00Z" ) ),
                                               null,
                                               null,
                                               false );

        PoolMetadata m3 = PoolMetadata.of( evaluation, poolThree );

        FeatureKey benchmarkLocation = FeatureKey.of( DRRC2 );

        Pool poolFour = MessageFactory.parse( new FeatureTuple( benchmarkLocation,
                                                                benchmarkLocation,
                                                                benchmarkLocation ),
                                              TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                                  Instant.parse( "1988-01-01T00:00:00Z" ) ),
                                              null,
                                              null,
                                              false );


        PoolMetadata benchmark = PoolMetadata.of( evaluation, poolFour );

        assertEquals( "Unexpected difference between union of metadata and benchmark.",
                      benchmark,
                      PoolMetadata.unionOf( Arrays.asList( m1, m2, m3 ) ) );
    }

    /**
     * Tests that the {@link PoolMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * null.
     */
    @Test
    public void testUnionOfThrowsExceptionWithNullInput()
    {
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> PoolMetadata.unionOf( null ) );

        assertEquals( "Cannot find the union of null metadata.", actual.getMessage() );
    }

    /**
     * Tests that the {@link PoolMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * empty.
     */
    @Test
    public void testUnionOfThrowsExceptionWithEmptyInput()
    {
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> PoolMetadata.unionOf( Collections.emptyList() ) );

        assertEquals( "Cannot find the union of empty input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link PoolMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * contains a null.
     */
    @Test
    public void testUnionOfThrowsExceptionWithOneNullInput()
    {
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> PoolMetadata.unionOf( Arrays.asList( (PoolMetadata) null ) ) );

        assertEquals( "Cannot find the union of null metadata.", actual.getMessage() );
    }

    /**
     * Tests that the {@link PoolMetadata#unionOf(java.util.List)} throws an expected exception when the inputs are
     * unequal on attributes that are expected to be equal.
     */
    @Test
    public void testUnionOfThrowsExceptionWithUnequalInputs()
    {
        FeatureTuple drrc3 = new FeatureTuple( FeatureKey.of( DRRC3 ),
                                               FeatureKey.of( DRRC3 ),
                                               FeatureKey.of( DRRC3 ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool poolOne = MessageFactory.parse( drrc3, null, null, null, false );

        PoolMetadata failOne = PoolMetadata.of( evaluation, poolOne );

        FeatureTuple a = new FeatureTuple( FeatureKey.of( "A" ),
                                           FeatureKey.of( "A" ),
                                           FeatureKey.of( "A" ) );

        Pool poolTwo = MessageFactory.parse( a, null, null, null, false );

        PoolMetadata failTwo = PoolMetadata.of( evaluation, poolTwo );

        PoolMetadataException actual = assertThrows( PoolMetadataException.class,
                                                       () -> PoolMetadata.unionOf( Arrays.asList( failOne,
                                                                                                    failTwo ) ) );

        assertEquals( "Only the time window and thresholds can differ when finding the union of metadata.",
                      actual.getMessage() );
    }

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
        FeatureKey a = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "B" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool poolOne = MessageFactory.parse( new FeatureTuple( a, a, a ), null, null, null, false );

        assertNotNull( PoolMetadata.of( evaluation, poolOne ) );

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                  Operator.EQUAL,
                                                                                  ThresholdDataType.LEFT ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), thresholds ) );

        TimeWindowOuter timeWindow =
                TimeWindowOuter.of( Instant.parse( THIRD_TIME ), Instant.parse( THIRD_TIME ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), timeWindow ) );

        assertNotNull( PoolMetadata.of( PoolMetadata.of(), timeWindow, thresholds ) );

        Pool pool = MessageFactory.parse( new FeatureTuple( a, a, a ),
                                          timeWindow,
                                          TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                             TimeScaleFunction.MEAN ),
                                          thresholds,
                                          false );

        assertNotNull( PoolMetadata.of( evaluation, pool ) );
    }

    /**
     * Test {@link PoolMetadata#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertEquals( PoolMetadata.of(), PoolMetadata.of() );
        FeatureKey l1 = FeatureKey.of( DRRC2 );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( l1, l1, l1 ), null, null, null, false );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );

        // Reflexive
        assertEquals( m1, m1 );
        FeatureKey l2 = FeatureKey.of( DRRC2 );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ), null, null, null, false );

        PoolMetadata m2 = PoolMetadata.of( evaluationTwo, poolTwo );

        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );
        FeatureKey l3 = FeatureKey.of( DRRC2 );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( SQIN )
                                               .setRightDataName( HEFS )
                                               .setMeasurementUnit( TEST_DIMENSION )
                                               .build();

        Pool poolThree = MessageFactory.parse( new FeatureTuple( l3, l3, l3 ), null, null, null, false );

        PoolMetadata m3 = PoolMetadata.of( evaluationThree, poolThree );

        FeatureKey l4 = FeatureKey.of( DRRC2 );

        Evaluation evaluationFour = Evaluation.newBuilder()
                                              .setRightVariableName( SQIN )
                                              .setRightDataName( HEFS )
                                              .setMeasurementUnit( TEST_DIMENSION )
                                              .build();

        Pool poolFour = MessageFactory.parse( new FeatureTuple( l4, l4, l4 ), null, null, null, false );

        PoolMetadata m4 = PoolMetadata.of( evaluationFour, poolFour );
        assertEquals( m3, m4 );
        assertNotEquals( m1, m3 );

        // Transitive
        FeatureKey l4t = FeatureKey.of( DRRC2 );

        Evaluation evaluationFive = Evaluation.newBuilder()
                                              .setRightVariableName( SQIN )
                                              .setRightDataName( HEFS )
                                              .setMeasurementUnit( TEST_DIMENSION )
                                              .build();

        Pool poolFive = MessageFactory.parse( new FeatureTuple( l4t, l4t, l4t ), null, null, null, false );

        PoolMetadata m4t = PoolMetadata.of( evaluationFive, poolFive );
        assertEquals( m4, m4t );
        assertEquals( m3, m4t );

        // Unequal
        FeatureKey l5 = FeatureKey.of( DRRC3 );
        Pool poolSix = MessageFactory.parse( new FeatureTuple( l5, l5, l5 ), null, null, null, false );
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
        FeatureKey l6 = FeatureKey.of( DRRC3 );

        Pool poolSeven = MessageFactory.parse( new FeatureTuple( l6, l6, l6 ),
                                               firstWindow,
                                               null,
                                               null,
                                               false );

        PoolMetadata m6 = PoolMetadata.of( evaluationFive, poolSeven );

        FeatureKey l7 = FeatureKey.of( DRRC3 );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );

        Pool poolEight = MessageFactory.parse( new FeatureTuple( l7, l7, l7 ),
                                               secondWindow,
                                               null,
                                               null,
                                               false );

        PoolMetadata m7 = PoolMetadata.of( evaluationFive, poolEight );

        assertEquals( m6, m7 );
        assertEquals( m7, m6 );
        assertNotEquals( m3, m6 );

        TimeWindowOuter thirdWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ),
                                                          Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        FeatureKey l8 = FeatureKey.of( DRRC3 );

        Pool poolNine = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                              thirdWindow,
                                              null,
                                              null,
                                              false );

        PoolMetadata m8 = PoolMetadata.of( evaluationFive, poolNine );

        assertNotEquals( m6, m8 );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Pool poolTen = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                             thirdWindow,
                                             null,
                                             thresholds,
                                             false );

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
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
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
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
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

        Pool poolEleven = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                                timeWindow,
                                                null,
                                                thresholds1,
                                                false );

        PoolMetadata m11 = PoolMetadata.of( evaluationSix, poolEleven );

        TimeWindowOuter timeWindow1 = thirdWindow;
        OneOrTwoThresholds thresholds2 = thresholds;

        ProjectConfigPlus mockConfigTwoPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockConfigTwoPlus.getProjectConfig() )
               .thenReturn( mockConfigTwo );

        Evaluation evaluationSeven = MessageFactory.parse( mockConfigTwoPlus );

        Pool poolTwelve = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                                timeWindow1,
                                                null,
                                                thresholds2,
                                                false );

        PoolMetadata m12 = PoolMetadata.of( evaluationSeven, poolTwelve );

        assertEquals( m11, m12 );

        // Add a time scale
        Pool poolThirteen = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                                  timeWindow1,
                                                  TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ),
                                                  thresholds2,
                                                  false );


        PoolMetadata m13 = PoolMetadata.of( evaluationSeven, poolThirteen );

        PoolMetadata m14 = PoolMetadata.of( evaluationSeven, poolThirteen );

        Pool poolFourteen = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                                  timeWindow1,
                                                  TimeScaleOuter.of( Duration.ofDays( 2 ), TimeScaleFunction.MEAN ),
                                                  thresholds2,
                                                  false );


        PoolMetadata m15 = PoolMetadata.of( evaluationSeven, poolFourteen );

        assertEquals( m13, m14 );

        assertNotEquals( m13, m15 );

        // Null check
        assertNotEquals( null, m6 );

        // Other type check
        assertNotEquals( m6, Double.valueOf( 2 ) );
    }

    /**
     * Test {@link PoolMetadata#equalsWithoutTimeWindowOrThresholds(PoolMetadata)}.
     */

    @Test
    public void testEqualsWithoutTimeWindowOrThresholds()
    {
        // False if the input is null
        assertFalse( PoolMetadata.of().equalsWithoutTimeWindowOrThresholds( null ) );

        // Different evaluations
        Evaluation evaluationOne = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        PoolMetadata one = PoolMetadata.of( evaluationOne, Pool.getDefaultInstance() );
        PoolMetadata two = PoolMetadata.of( evaluationTwo, Pool.getDefaultInstance() );

        assertFalse( one.equalsWithoutTimeWindowOrThresholds( two ) );
    }

    /**
     * Test {@link PoolMetadata#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertEquals( PoolMetadata.of().hashCode(), PoolMetadata.of().hashCode() );
        FeatureKey l1 = FeatureKey.of( DRRC2 );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( l1, l1, l1 ), null, null, null, false );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );
        assertEquals( m1.hashCode(), m1.hashCode() );
        FeatureKey l2 = FeatureKey.of( DRRC2 );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ), null, null, null, false );

        PoolMetadata m2 = PoolMetadata.of( evaluationTwo, poolTwo );
        assertEquals( m1.hashCode(), m2.hashCode() );
        FeatureKey l3 = FeatureKey.of( DRRC2 );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( SQIN )
                                               .setRightDataName( HEFS )
                                               .setMeasurementUnit( PoolMetadataTest.TEST_DIMENSION )
                                               .build();

        Pool poolThree = MessageFactory.parse( new FeatureTuple( l3, l3, l3 ), null, null, null, false );

        PoolMetadata m3 = PoolMetadata.of( evaluationThree, poolThree );
        FeatureKey l4 = FeatureKey.of( DRRC2 );

        Pool poolFour = MessageFactory.parse( new FeatureTuple( l4, l4, l4 ), null, null, null, false );

        PoolMetadata m4 = PoolMetadata.of( evaluationThree, poolFour );
        assertEquals( m3.hashCode(), m4.hashCode() );
        FeatureKey l4t = FeatureKey.of( DRRC2 );

        Pool poolFive = MessageFactory.parse( new FeatureTuple( l4t, l4t, l4t ), null, null, null, false );

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
        FeatureKey l6 = FeatureKey.of( DRRC3 );

        Pool poolSix = MessageFactory.parse( new FeatureTuple( l6, l6, l6 ),
                                             firstWindow,
                                             null,
                                             null,
                                             false );

        PoolMetadata m6 = PoolMetadata.of( evaluationThree, poolSix );

        FeatureKey l7 = FeatureKey.of( DRRC3 );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );

        Pool poolSeven = MessageFactory.parse( new FeatureTuple( l7, l7, l7 ),
                                               secondWindow,
                                               null,
                                               null,
                                               false );

        PoolMetadata m7 = PoolMetadata.of( evaluationThree, poolSeven );

        assertEquals( m6.hashCode(), m7.hashCode() );
        assertEquals( m7.hashCode(), m6.hashCode() );

        TimeWindowOuter thirdWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ),
                                                          Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        FeatureKey l8 = FeatureKey.of( DRRC3 );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Pool poolEight = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                               thirdWindow,
                                               null,
                                               thresholds,
                                               false );

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
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
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
                                                   null ),
                                   Arrays.asList( new MetricsConfig( null,
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

        Pool poolNine = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                              timeWindow,
                                              null,
                                              thresholds1,
                                              false );

        PoolMetadata m11 = PoolMetadata.of( evaluationFour, poolNine );

        ProjectConfigPlus mockConfigTwoPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockConfigTwoPlus.getProjectConfig() )
               .thenReturn( mockConfigTwo );

        Evaluation evaluationFive = MessageFactory.parse( mockConfigTwoPlus );

        Pool poolTen = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                             thirdWindow,
                                             null,
                                             thresholds,
                                             false );

        PoolMetadata m12 = PoolMetadata.of( evaluationFive, poolTen );

        assertEquals( m11.hashCode(), m12.hashCode() );
    }
}
