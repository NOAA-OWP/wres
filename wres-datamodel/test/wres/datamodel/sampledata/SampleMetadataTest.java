package wres.datamodel.sampledata;

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
 * Tests the {@link SampleMetadata}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SampleMetadataTest
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
     * Tests the {@link SampleMetadata#unionOf(java.util.List)} against a benchmark.
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

        SampleMetadata m1 = SampleMetadata.of( evaluation, poolOne );

        FeatureKey l2 = FeatureKey.of( DRRC2 );

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ),
                                             TimeWindowOuter.of( Instant.parse( SECOND_TIME ),
                                                                 Instant.parse( "1986-12-31T23:59:59Z" ) ),
                                             null,
                                             null,
                                             false );

        SampleMetadata m2 = SampleMetadata.of( evaluation, poolTwo );

        FeatureKey l3 = FeatureKey.of( DRRC2 );

        Pool poolThree = MessageFactory.parse( new FeatureTuple( l3, l3, l3 ),
                                               TimeWindowOuter.of( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                   Instant.parse( "1988-01-01T00:00:00Z" ) ),
                                               null,
                                               null,
                                               false );

        SampleMetadata m3 = SampleMetadata.of( evaluation, poolThree );

        FeatureKey benchmarkLocation = FeatureKey.of( DRRC2 );

        Pool poolFour = MessageFactory.parse( new FeatureTuple( benchmarkLocation,
                                                                benchmarkLocation,
                                                                benchmarkLocation ),
                                              TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                                  Instant.parse( "1988-01-01T00:00:00Z" ) ),
                                              null,
                                              null,
                                              false );


        SampleMetadata benchmark = SampleMetadata.of( evaluation, poolFour );

        assertEquals( "Unexpected difference between union of metadata and benchmark.",
                      benchmark,
                      SampleMetadata.unionOf( Arrays.asList( m1, m2, m3 ) ) );
    }

    /**
     * Tests that the {@link SampleMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * null.
     */
    @Test
    public void testUnionOfThrowsExceptionWithNullInput()
    {
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> SampleMetadata.unionOf( null ) );

        assertEquals( "Cannot find the union of null metadata.", actual.getMessage() );
    }

    /**
     * Tests that the {@link SampleMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * empty.
     */
    @Test
    public void testUnionOfThrowsExceptionWithEmptyInput()
    {
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> SampleMetadata.unionOf( Collections.emptyList() ) );

        assertEquals( "Cannot find the union of empty input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link SampleMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * contains a null.
     */
    @Test
    public void testUnionOfThrowsExceptionWithOneNullInput()
    {
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> SampleMetadata.unionOf( Arrays.asList( (SampleMetadata) null ) ) );

        assertEquals( "Cannot find the union of null metadata.", actual.getMessage() );
    }

    /**
     * Tests that the {@link SampleMetadata#unionOf(java.util.List)} throws an expected exception when the inputs are
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

        SampleMetadata failOne = SampleMetadata.of( evaluation, poolOne );

        FeatureTuple a = new FeatureTuple( FeatureKey.of( "A" ),
                                           FeatureKey.of( "A" ),
                                           FeatureKey.of( "A" ) );

        Pool poolTwo = MessageFactory.parse( a, null, null, null, false );

        SampleMetadata failTwo = SampleMetadata.of( evaluation, poolTwo );

        SampleMetadataException actual = assertThrows( SampleMetadataException.class,
                                                       () -> SampleMetadata.unionOf( Arrays.asList( failOne,
                                                                                                    failTwo ) ) );

        assertEquals( "Only the time window and thresholds can differ when finding the union of metadata.",
                      actual.getMessage() );
    }

    /**
     * Tests construction of the {@link SampleMetadata} using the various construction options.
     */

    @Test
    public void testBuildProducesNonNullInstances()
    {
        assertNotNull( SampleMetadata.of() );

        assertNotNull( SampleMetadata.of( Evaluation.newBuilder()
                                                    .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                                    .build(),
                                          Pool.getDefaultInstance() ) );
        FeatureKey a = FeatureKey.of( "A" );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "B" )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool poolOne = MessageFactory.parse( new FeatureTuple( a, a, a ), null, null, null, false );

        assertNotNull( SampleMetadata.of( evaluation, poolOne ) );

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                  Operator.EQUAL,
                                                                                  ThresholdDataType.LEFT ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), thresholds ) );

        TimeWindowOuter timeWindow =
                TimeWindowOuter.of( Instant.parse( THIRD_TIME ), Instant.parse( THIRD_TIME ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), timeWindow ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), timeWindow, thresholds ) );

        Pool pool = MessageFactory.parse( new FeatureTuple( a, a, a ),
                                          timeWindow,
                                          TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                             TimeScaleFunction.MEAN ),
                                          thresholds,
                                          false );

        assertNotNull( SampleMetadata.of( evaluation, pool ) );
    }

    /**
     * Test {@link SampleMetadata#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertEquals( SampleMetadata.of(), SampleMetadata.of() );
        FeatureKey l1 = FeatureKey.of( DRRC2 );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( l1, l1, l1 ), null, null, null, false );

        SampleMetadata m1 = SampleMetadata.of( evaluation, pool );

        // Reflexive
        assertEquals( m1, m1 );
        FeatureKey l2 = FeatureKey.of( DRRC2 );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ), null, null, null, false );

        SampleMetadata m2 = SampleMetadata.of( evaluationTwo, poolTwo );

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

        SampleMetadata m3 = SampleMetadata.of( evaluationThree, poolThree );

        FeatureKey l4 = FeatureKey.of( DRRC2 );

        Evaluation evaluationFour = Evaluation.newBuilder()
                                              .setRightVariableName( SQIN )
                                              .setRightDataName( HEFS )
                                              .setMeasurementUnit( TEST_DIMENSION )
                                              .build();

        Pool poolFour = MessageFactory.parse( new FeatureTuple( l4, l4, l4 ), null, null, null, false );

        SampleMetadata m4 = SampleMetadata.of( evaluationFour, poolFour );
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

        SampleMetadata m4t = SampleMetadata.of( evaluationFive, poolFive );
        assertEquals( m4, m4t );
        assertEquals( m3, m4t );

        // Unequal
        FeatureKey l5 = FeatureKey.of( DRRC3 );
        Pool poolSix = MessageFactory.parse( new FeatureTuple( l5, l5, l5 ), null, null, null, false );
        SampleMetadata m5 = SampleMetadata.of( evaluationFive, poolSix );
        assertNotEquals( m4, m5 );
        SampleMetadata m5NoDim = SampleMetadata.of( Evaluation.newBuilder()
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

        SampleMetadata m6 = SampleMetadata.of( evaluationFive, poolSeven );

        FeatureKey l7 = FeatureKey.of( DRRC3 );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );

        Pool poolEight = MessageFactory.parse( new FeatureTuple( l7, l7, l7 ),
                                               secondWindow,
                                               null,
                                               null,
                                               false );

        SampleMetadata m7 = SampleMetadata.of( evaluationFive, poolEight );

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

        SampleMetadata m8 = SampleMetadata.of( evaluationFive, poolNine );

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

        SampleMetadata m9 = SampleMetadata.of( evaluationFive, poolTen );

        assertNotEquals( m8, m9 );

        SampleMetadata m10 = SampleMetadata.of( evaluationFive, poolTen );

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

        SampleMetadata m11 = SampleMetadata.of( evaluationSix, poolEleven );

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

        SampleMetadata m12 = SampleMetadata.of( evaluationSeven, poolTwelve );

        assertEquals( m11, m12 );

        // Add a time scale
        Pool poolThirteen = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                                  timeWindow1,
                                                  TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ),
                                                  thresholds2,
                                                  false );


        SampleMetadata m13 = SampleMetadata.of( evaluationSeven, poolThirteen );

        SampleMetadata m14 = SampleMetadata.of( evaluationSeven, poolThirteen );

        Pool poolFourteen = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                                  timeWindow1,
                                                  TimeScaleOuter.of( Duration.ofDays( 2 ), TimeScaleFunction.MEAN ),
                                                  thresholds2,
                                                  false );


        SampleMetadata m15 = SampleMetadata.of( evaluationSeven, poolFourteen );

        assertEquals( m13, m14 );

        assertNotEquals( m13, m15 );

        // Null check
        assertNotEquals( null, m6 );

        // Other type check
        assertNotEquals( m6, Double.valueOf( 2 ) );
    }

    /**
     * Test {@link SampleMetadata#equalsWithoutTimeWindowOrThresholds(SampleMetadata)}.
     */

    @Test
    public void testEqualsWithoutTimeWindowOrThresholds()
    {
        // False if the input is null
        assertFalse( SampleMetadata.of().equalsWithoutTimeWindowOrThresholds( null ) );

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

        SampleMetadata one = SampleMetadata.of( evaluationOne, Pool.getDefaultInstance() );
        SampleMetadata two = SampleMetadata.of( evaluationTwo, Pool.getDefaultInstance() );

        assertFalse( one.equalsWithoutTimeWindowOrThresholds( two ) );
    }

    /**
     * Test {@link SampleMetadata#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertEquals( SampleMetadata.of().hashCode(), SampleMetadata.of().hashCode() );
        FeatureKey l1 = FeatureKey.of( DRRC2 );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( SQIN )
                                          .setRightDataName( HEFS )
                                          .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( l1, l1, l1 ), null, null, null, false );

        SampleMetadata m1 = SampleMetadata.of( evaluation, pool );
        assertEquals( m1.hashCode(), m1.hashCode() );
        FeatureKey l2 = FeatureKey.of( DRRC2 );

        Evaluation evaluationTwo = Evaluation.newBuilder()
                                             .setRightVariableName( SQIN )
                                             .setRightDataName( HEFS )
                                             .setMeasurementUnit( MeasurementUnit.DIMENSIONLESS )
                                             .build();

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( l2, l2, l2 ), null, null, null, false );

        SampleMetadata m2 = SampleMetadata.of( evaluationTwo, poolTwo );
        assertEquals( m1.hashCode(), m2.hashCode() );
        FeatureKey l3 = FeatureKey.of( DRRC2 );

        Evaluation evaluationThree = Evaluation.newBuilder()
                                               .setRightVariableName( SQIN )
                                               .setRightDataName( HEFS )
                                               .setMeasurementUnit( SampleMetadataTest.TEST_DIMENSION )
                                               .build();

        Pool poolThree = MessageFactory.parse( new FeatureTuple( l3, l3, l3 ), null, null, null, false );

        SampleMetadata m3 = SampleMetadata.of( evaluationThree, poolThree );
        FeatureKey l4 = FeatureKey.of( DRRC2 );

        Pool poolFour = MessageFactory.parse( new FeatureTuple( l4, l4, l4 ), null, null, null, false );

        SampleMetadata m4 = SampleMetadata.of( evaluationThree, poolFour );
        assertEquals( m3.hashCode(), m4.hashCode() );
        FeatureKey l4t = FeatureKey.of( DRRC2 );

        Pool poolFive = MessageFactory.parse( new FeatureTuple( l4t, l4t, l4t ), null, null, null, false );

        SampleMetadata m4t = SampleMetadata.of( evaluationThree, poolFive );
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

        SampleMetadata m6 = SampleMetadata.of( evaluationThree, poolSix );

        FeatureKey l7 = FeatureKey.of( DRRC3 );

        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );

        Pool poolSeven = MessageFactory.parse( new FeatureTuple( l7, l7, l7 ),
                                               secondWindow,
                                               null,
                                               null,
                                               false );

        SampleMetadata m7 = SampleMetadata.of( evaluationThree, poolSeven );

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

        SampleMetadata m9 = SampleMetadata.of( evaluationThree, poolEight );

        SampleMetadata m10 = SampleMetadata.of( evaluationThree, poolEight );

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

        SampleMetadata m11 = SampleMetadata.of( evaluationFour, poolNine );

        ProjectConfigPlus mockConfigTwoPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( mockConfigTwoPlus.getProjectConfig() )
               .thenReturn( mockConfigTwo );

        Evaluation evaluationFive = MessageFactory.parse( mockConfigTwoPlus );

        Pool poolTen = MessageFactory.parse( new FeatureTuple( l8, l8, l8 ),
                                             thirdWindow,
                                             null,
                                             thresholds,
                                             false );

        SampleMetadata m12 = SampleMetadata.of( evaluationFive, poolTen );

        assertEquals( m11.hashCode(), m12.hashCode() );
    }
}
