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

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.SampleMetadata.Builder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;

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
        SampleMetadata m1 = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ), SQIN, HEFS ) )
                                         .setTimeWindow( TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                                             Instant.parse( "1985-12-31T23:59:59Z" ) ) )
                                         .build();
        FeatureKey l2 = FeatureKey.of( DRRC2 );
        SampleMetadata m2 = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ), SQIN, HEFS ) )
                                         .setTimeWindow( TimeWindowOuter.of( Instant.parse( SECOND_TIME ),
                                                                             Instant.parse( "1986-12-31T23:59:59Z" ) ) )
                                         .build();
        FeatureKey l3 = FeatureKey.of( DRRC2 );
        SampleMetadata m3 = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l3, l3, l3 ), SQIN, HEFS ) )
                                         .setTimeWindow( TimeWindowOuter.of( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                             Instant.parse( "1988-01-01T00:00:00Z" ) ) )
                                         .build();
        FeatureKey benchmarkLocation = FeatureKey.of( DRRC2 );
        SampleMetadata benchmark = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                                .setIdentifier( DatasetIdentifier.of( new FeatureTuple( benchmarkLocation, benchmarkLocation, benchmarkLocation ),
                                                                                      SQIN,
                                                                                      HEFS ) )
                                                .setTimeWindow( TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                                                    Instant.parse( "1988-01-01T00:00:00Z" ) ) )
                                                .build();

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
        SampleMetadata failOne = SampleMetadata.of( MeasurementUnit.of(),
                                                    DatasetIdentifier.of( drrc3, SQIN, HEFS ) );
        FeatureTuple a = new FeatureTuple( FeatureKey.of( "A" ),
                                           FeatureKey.of( "A" ),
                                           FeatureKey.of( "A" ) );
        SampleMetadata failTwo =
                SampleMetadata.of( MeasurementUnit.of(), DatasetIdentifier.of( a, "B" ) );

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

        assertNotNull( SampleMetadata.of( MeasurementUnit.of() ) );
        FeatureKey a = FeatureKey.of( "A" );
        DatasetIdentifier identifier = DatasetIdentifier.of( new FeatureTuple( a, a, a ), "B" );

        assertNotNull( SampleMetadata.of( MeasurementUnit.of(), identifier ) );

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                  Operator.EQUAL,
                                                                                  ThresholdDataType.LEFT ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), thresholds ) );

        TimeWindowOuter timeWindow =
                TimeWindowOuter.of( Instant.parse( THIRD_TIME ), Instant.parse( THIRD_TIME ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), timeWindow ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), timeWindow, thresholds ) );

        assertNotNull( SampleMetadata.of( MeasurementUnit.of(), identifier, timeWindow, thresholds ) );

        assertNotNull( new Builder().setIdentifier( identifier )
                                    .setMeasurementUnit( MeasurementUnit.of() )
                                    .setProjectConfig( new ProjectConfig( null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null ) )
                                    .setThresholds( thresholds )
                                    .setTimeWindow( timeWindow )
                                    .setTimeScale( TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                                      TimeScaleFunction.MEAN ) )
                                    .build() );
    }

    /**
     * Test {@link SampleMetadata#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertEquals( SampleMetadata.of(), SampleMetadata.of() );
        FeatureKey l1 = FeatureKey.of( DRRC2 );
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ), SQIN, HEFS ) );
        // Reflexive
        assertEquals( m1, m1 );
        FeatureKey l2 = FeatureKey.of( DRRC2 );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ), SQIN, HEFS ) );
        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );
        FeatureKey l3 = FeatureKey.of( DRRC2 );
        SampleMetadata m3 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( new FeatureTuple( l3, l3, l3 ), SQIN, HEFS ) );
        FeatureKey l4 = FeatureKey.of( DRRC2 );
        SampleMetadata m4 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( new FeatureTuple( l4, l4, l4 ), SQIN, HEFS ) );
        assertEquals( m3, m4 );
        assertNotEquals( m1, m3 );
        // Transitive
        FeatureKey l4t = FeatureKey.of( DRRC2 );
        SampleMetadata m4t = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( new FeatureTuple( l4t, l4t, l4t), SQIN, HEFS ) );
        assertEquals( m4, m4t );
        assertEquals( m3, m4t );
        // Unequal
        FeatureKey l5 = FeatureKey.of( DRRC3 );
        SampleMetadata m5 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( new FeatureTuple( l5, l5, l5 ), SQIN, HEFS ) );
        assertNotEquals( m4, m5 );
        SampleMetadata m5NoDim = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ), null );
        assertNotEquals( m5, m5NoDim );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( m1.equals( m2 ) );
        }
        // Add a time window
        TimeWindowOuter firstWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );
        FeatureKey l6 = FeatureKey.of( DRRC3 );
        final TimeWindowOuter timeWindow2 = firstWindow;
        SampleMetadata m6 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l6, l6, l6 ), SQIN, HEFS ) )
                                         .setTimeWindow( timeWindow2 )
                                         .build();
        FeatureKey l7 = FeatureKey.of( DRRC3 );
        final TimeWindowOuter timeWindow3 = secondWindow;
        SampleMetadata m7 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l7, l7, l7 ), SQIN, HEFS ) )
                                         .setTimeWindow( timeWindow3 )
                                         .build();
        assertEquals( m6, m7 );
        assertEquals( m7, m6 );
        assertNotEquals( m3, m6 );

        TimeWindowOuter thirdWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ),
                                                          Instant.parse( FIRST_TIME ),
                                                          Instant.parse( SECOND_TIME ) );
        FeatureKey l8 = FeatureKey.of( DRRC3 );
        final TimeWindowOuter timeWindow4 = thirdWindow;
        SampleMetadata m8 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ) )
                                         .setTimeWindow( timeWindow4 )
                                         .build();
        assertNotEquals( m6, m8 );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        SampleMetadata m9 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ),
                                               thirdWindow,
                                               thresholds );

        assertNotEquals( m8, m9 );

        SampleMetadata m10 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ),
                                                thirdWindow,
                                                thresholds );

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
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      null,
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
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      null,
                                                                                                      MetricConfigName.BIAS_FRACTION ) ),
                                                                     null ) ),
                                   null,
                                   null,
                                   null );
        final TimeWindowOuter timeWindow = thirdWindow;
        final OneOrTwoThresholds thresholds1 = thresholds;
        final ProjectConfig projectConfig = mockConfigOne;

        SampleMetadata m11 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                          .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ) )
                                          .setTimeWindow( timeWindow )
                                          .setThresholds( thresholds1 )
                                          .setProjectConfig( projectConfig )
                                          .build();
        final TimeWindowOuter timeWindow1 = thirdWindow;
        final OneOrTwoThresholds thresholds2 = thresholds;
        final ProjectConfig projectConfig1 = mockConfigTwo;

        SampleMetadata m12 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                          .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ) )
                                          .setTimeWindow( timeWindow1 )
                                          .setThresholds( thresholds2 )
                                          .setProjectConfig( projectConfig1 )
                                          .build();

        assertEquals( m11, m12 );

        // Add a time scale
        SampleMetadata m13 =
                new Builder( m12 ).setTimeScale( TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ) )
                                  .build();

        SampleMetadata m14 =
                new Builder( m12 ).setTimeScale( TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ) )
                                  .build();

        SampleMetadata m15 =
                new Builder( m12 ).setTimeScale( TimeScaleOuter.of( Duration.ofDays( 2 ), TimeScaleFunction.MEAN ) )
                                  .build();

        assertEquals( m13, m14 );

        assertNotEquals( m13, m15 );

        // Null check
        assertNotEquals( null, m6 );

        // Other type check
        assertNotEquals( Double.valueOf( 2 ), m6 );
    }

    /**
     * Test {@link SampleMetadata#equalsWithoutTimeWindowOrThresholds(SampleMetadata)}.
     */

    @Test
    public void testEqualsWithoutTimeWindowOrThresholds()
    {
        // False if the input is null
        assertFalse( SampleMetadata.of().equalsWithoutTimeWindowOrThresholds( null ) );

        // Simplest case of equality
        assertTrue( SampleMetadata.of().equalsWithoutTimeWindowOrThresholds( SampleMetadata.of() ) );

        // Remaining cases test scenarios not tested already through Object::equals
        SampleMetadata metaOne = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                              .setProjectConfig( new ProjectConfig( null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null ) )
                                              .setTimeScale( TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                                                TimeScaleFunction.MAXIMUM ) )
                                              .build();

        SampleMetadata metaTwo = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                              .build();

        assertTrue( metaOne.equalsWithoutTimeWindowOrThresholds( metaOne ) );

        assertFalse( metaOne.equalsWithoutTimeWindowOrThresholds( metaTwo ) );

        SampleMetadata metaThree = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                               .setTimeScale( TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                                                 TimeScaleFunction.MEAN ) )
                                               .build();

        SampleMetadata metaFour = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                               .setTimeScale( TimeScaleOuter.of( Duration.ofDays( 1 ),
                                                                                 TimeScaleFunction.MAXIMUM ) )
                                               .build();

        SampleMetadata metaFive = new Builder().setMeasurementUnit( MeasurementUnit.of() )
                                              .setProjectConfig( new ProjectConfig( null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null,
                                                                                    null ) )
                                              .build();

        assertFalse( metaThree.equalsWithoutTimeWindowOrThresholds( metaOne ) );

        assertTrue( metaThree.equalsWithoutTimeWindowOrThresholds( metaThree ) );

        assertFalse( metaThree.equalsWithoutTimeWindowOrThresholds( metaFour ) );

        assertFalse( metaFive.equalsWithoutTimeWindowOrThresholds( metaOne ) );
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
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( new FeatureTuple( l1, l1, l1 ), SQIN, HEFS ) );
        assertEquals( m1.hashCode(), m1.hashCode() );
        FeatureKey l2 = FeatureKey.of( DRRC2 );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( new FeatureTuple( l2, l2, l2 ), SQIN, HEFS ) );
        assertEquals( m1.hashCode(), m2.hashCode() );
        FeatureKey l3 = FeatureKey.of( DRRC2 );
        SampleMetadata m3 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( new FeatureTuple( l3, l3, l3 ), SQIN, HEFS ) );
        FeatureKey l4 = FeatureKey.of( DRRC2 );
        SampleMetadata m4 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( new FeatureTuple( l4, l4, l4 ), SQIN, HEFS ) );
        assertEquals( m3.hashCode(), m4.hashCode() );
        FeatureKey l4t = FeatureKey.of( DRRC2 );
        SampleMetadata m4t = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( new FeatureTuple( l4t, l4t, l4t ), SQIN, HEFS ) );
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
        TimeWindowOuter secondWindow = TimeWindowOuter.of( Instant.parse( FIRST_TIME ),
                                                           Instant.parse( SECOND_TIME ) );
        FeatureKey l6 = FeatureKey.of( DRRC3 );
        final TimeWindowOuter timeWindow2 = firstWindow;
        SampleMetadata m6 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l6, l6, l6 ), SQIN, HEFS ) )
                                         .setTimeWindow( timeWindow2 )
                                         .build();
        FeatureKey l7 = FeatureKey.of( DRRC3 );
        final TimeWindowOuter timeWindow3 = secondWindow;
        SampleMetadata m7 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                         .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l7, l7, l7 ), SQIN, HEFS ) )
                                         .setTimeWindow( timeWindow3 )
                                         .build();
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

        SampleMetadata m9 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ),
                                               thirdWindow,
                                               thresholds );

        SampleMetadata m10 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ),
                                                thirdWindow,
                                                thresholds );

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
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      null,
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
                                                                     null ),
                                               null ),
                                   null,
                                   Arrays.asList( new MetricsConfig( null,
                                                                     Arrays.asList( new MetricConfig( null,
                                                                                                      null,
                                                                                                      MetricConfigName.BIAS_FRACTION ) ),
                                                                     null ) ),
                                   null,
                                   null,
                                   null );
        final TimeWindowOuter timeWindow = thirdWindow;
        final OneOrTwoThresholds thresholds1 = thresholds;
        final ProjectConfig projectConfig = mockConfigOne;

        SampleMetadata m11 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                          .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ) )
                                          .setTimeWindow( timeWindow )
                                          .setThresholds( thresholds1 )
                                          .setProjectConfig( projectConfig )
                                          .build();
        final TimeWindowOuter timeWindow1 = thirdWindow;
        final OneOrTwoThresholds thresholds2 = thresholds;
        final ProjectConfig projectConfig1 = mockConfigTwo;

        SampleMetadata m12 = new Builder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                          .setIdentifier( DatasetIdentifier.of( new FeatureTuple( l8, l8, l8 ), SQIN, HEFS ) )
                                          .setTimeWindow( timeWindow1 )
                                          .setThresholds( thresholds2 )
                                          .setProjectConfig( projectConfig1 )
                                          .build();

        assertEquals( m11.hashCode(), m12.hashCode() );
    }
}
