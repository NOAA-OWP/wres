package wres.datamodel.sampledata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

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
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindow;

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

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link SampleMetadata#unionOf(java.util.List)} against a benchmark.
     */
    @Test
    public void unionOf()
    {
        Location l1 = Location.of( DRRC2 );
        SampleMetadata m1 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                       .setIdentifier( DatasetIdentifier.of( l1, SQIN, HEFS ) )
                                                       .setTimeWindow( TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                                                      Instant.parse( "1985-12-31T23:59:59Z" ) ) )
                                                       .build();
        Location l2 = Location.of( DRRC2 );
        SampleMetadata m2 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                       .setIdentifier( DatasetIdentifier.of( l2, SQIN, HEFS ) )
                                                       .setTimeWindow( TimeWindow.of( Instant.parse( SECOND_TIME ),
                                                                                      Instant.parse( "1986-12-31T23:59:59Z" ) ) )
                                                       .build();
        Location l3 = Location.of( DRRC2 );
        SampleMetadata m3 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                       .setIdentifier( DatasetIdentifier.of( l3, SQIN, HEFS ) )
                                                       .setTimeWindow( TimeWindow.of( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                                      Instant.parse( "1988-01-01T00:00:00Z" ) ) )
                                                       .build();
        Location benchmarkLocation = Location.of( DRRC2 );
        SampleMetadata benchmark = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                              .setIdentifier( DatasetIdentifier.of( benchmarkLocation,
                                                                                                    SQIN,
                                                                                                    HEFS ) )
                                                              .setTimeWindow( TimeWindow.of( Instant.parse( FIRST_TIME ),
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
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot find the union of null metadata." );

        SampleMetadata.unionOf( null );
    }

    /**
     * Tests that the {@link SampleMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * empty.
     */
    @Test
    public void testUnionOfThrowsExceptionWithEmptyInput()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot find the union of empty input." );

        SampleMetadata.unionOf( Collections.emptyList() );
    }

    /**
     * Tests that the {@link SampleMetadata#unionOf(java.util.List)} throws an expected exception when the input is
     * contains a null.
     */
    @Test
    public void testUnionOfThrowsExceptionWithOneNullInput()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot find the union of null metadata." );

        SampleMetadata.unionOf( Arrays.asList( (SampleMetadata) null ) );
    }


    /**
     * Tests that the {@link SampleMetadata#unionOf(java.util.List)} throws an expected exception when the inputs are
     * unequal on attributes that are expected to be equal.
     */
    @Test
    public void testUnionOfThrowsExceptionWithUnequalInputs()
    {
        exception.expect( SampleMetadataException.class );
        exception.expectMessage( "Only the time window and thresholds can differ when finding the union of metadata." );

        SampleMetadata failOne = SampleMetadata.of( MeasurementUnit.of(),
                                                    DatasetIdentifier.of( Location.of( DRRC3 ), SQIN, HEFS ) );
        SampleMetadata failTwo =
                SampleMetadata.of( MeasurementUnit.of(), DatasetIdentifier.of( Location.of( "A" ), "B" ) );

        SampleMetadata.unionOf( Arrays.asList( failOne, failTwo ) );
    }

    /**
     * Tests construction of the {@link SampleMetadata} using the various construction options.
     */

    @Test
    public void testBuildProducesNonNullInstances()
    {
        assertNotNull( SampleMetadata.of() );

        assertNotNull( SampleMetadata.of( MeasurementUnit.of() ) );

        DatasetIdentifier identifier = DatasetIdentifier.of( Location.of( "A" ), "B" );

        assertNotNull( SampleMetadata.of( MeasurementUnit.of(), identifier ) );

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                             Operator.EQUAL,
                                                                             ThresholdDataType.LEFT ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), thresholds ) );

        TimeWindow timeWindow =
                TimeWindow.of( Instant.parse( THIRD_TIME ), Instant.parse( THIRD_TIME ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), timeWindow ) );

        assertNotNull( SampleMetadata.of( SampleMetadata.of(), timeWindow, thresholds ) );

        assertNotNull( SampleMetadata.of( MeasurementUnit.of(), identifier, timeWindow, thresholds ) );

        assertNotNull( new SampleMetadataBuilder().setFromExistingInstance( SampleMetadata.of() )
                                                  .setIdentifier( identifier )
                                                  .setMeasurementUnit( MeasurementUnit.of() )
                                                  .setProjectConfig( new ProjectConfig( null,
                                                                                        null,
                                                                                        null,
                                                                                        null,
                                                                                        null,
                                                                                        null ) )
                                                  .setThresholds( thresholds )
                                                  .setTimeWindow( timeWindow )
                                                  .setTimeScale( TimeScale.of( Duration.ofDays( 1 ),
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
        Location l1 = Location.of( DRRC2 );
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( l1, SQIN, HEFS ) );
        // Reflexive
        assertEquals( m1, m1 );
        Location l2 = Location.of( DRRC2 );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( l2, SQIN, HEFS ) );
        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );
        Location l3 = Location.of( DRRC2 );
        SampleMetadata m3 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( l3, SQIN, HEFS ) );
        Location l4 = Location.of( DRRC2 );
        SampleMetadata m4 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( l4, SQIN, HEFS ) );
        assertEquals( m3, m4 );
        assertNotEquals( m1, m3 );
        // Transitive
        Location l4t = Location.of( DRRC2 );
        SampleMetadata m4t = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( l4t, SQIN, HEFS ) );
        assertEquals( m4, m4t );
        assertEquals( m3, m4t );
        // Unequal
        Location l5 = Location.of( DRRC3 );
        SampleMetadata m5 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( l5, SQIN, HEFS ) );
        assertNotEquals( m4, m5 );
        SampleMetadata m5NoDim = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ), null );
        assertNotEquals( m5, m5NoDim );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( m1.equals( m2 ) );
        }
        // Add a time window
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                Instant.parse( SECOND_TIME ) );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ) );
        Location l6 = Location.of( DRRC3 );
        final TimeWindow timeWindow2 = firstWindow;
        SampleMetadata m6 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                       .setIdentifier( DatasetIdentifier.of( l6, SQIN, HEFS ) )
                                                       .setTimeWindow( timeWindow2 )
                                                       .build();
        Location l7 = Location.of( DRRC3 );
        final TimeWindow timeWindow3 = secondWindow;
        SampleMetadata m7 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                       .setIdentifier( DatasetIdentifier.of( l7, SQIN, HEFS ) )
                                                       .setTimeWindow( timeWindow3 )
                                                       .build();
        assertEquals( m6, m7 );
        assertEquals( m7, m6 );
        assertNotEquals( m3, m6 );

        TimeWindow thirdWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                Instant.parse( SECOND_TIME ),
                                                Instant.parse( FIRST_TIME ),
                                                Instant.parse( SECOND_TIME ) );
        Location l8 = Location.of( DRRC3 );
        final TimeWindow timeWindow4 = thirdWindow;
        SampleMetadata m8 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                       .setIdentifier( DatasetIdentifier.of( l8, SQIN, HEFS ) )
                                                       .setTimeWindow( timeWindow4 )
                                                       .build();
        assertNotEquals( m6, m8 );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        SampleMetadata m9 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( l8, SQIN, HEFS ),
                                               thirdWindow,
                                               thresholds );

        assertNotEquals( m8, m9 );

        SampleMetadata m10 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( l8, SQIN, HEFS ),
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
        final TimeWindow timeWindow = thirdWindow;
        final OneOrTwoThresholds thresholds1 = thresholds;
        final ProjectConfig projectConfig = mockConfigOne;

        SampleMetadata m11 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                        .setIdentifier( DatasetIdentifier.of( l8, SQIN, HEFS ) )
                                                        .setTimeWindow( timeWindow )
                                                        .setThresholds( thresholds1 )
                                                        .setProjectConfig( projectConfig )
                                                        .build();
        final TimeWindow timeWindow1 = thirdWindow;
        final OneOrTwoThresholds thresholds2 = thresholds;
        final ProjectConfig projectConfig1 = mockConfigTwo;

        SampleMetadata m12 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                        .setIdentifier( DatasetIdentifier.of( l8, SQIN, HEFS ) )
                                                        .setTimeWindow( timeWindow1 )
                                                        .setThresholds( thresholds2 )
                                                        .setProjectConfig( projectConfig1 )
                                                        .build();

        assertEquals( m11, m12 );

        // Add a time scale
        SampleMetadata m13 =
                new SampleMetadataBuilder().setFromExistingInstance( m12 )
                                           .setTimeScale( TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ) )
                                           .build();

        SampleMetadata m14 =
                new SampleMetadataBuilder().setFromExistingInstance( m12 )
                                           .setTimeScale( TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ) )
                                           .build();

        SampleMetadata m15 =
                new SampleMetadataBuilder().setFromExistingInstance( m12 )
                                           .setTimeScale( TimeScale.of( Duration.ofDays( 2 ), TimeScaleFunction.MEAN ) )
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
        SampleMetadata metaOne = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                            .setProjectConfig( new ProjectConfig( null,
                                                                                                  null,
                                                                                                  null,
                                                                                                  null,
                                                                                                  null,
                                                                                                  null ) )
                                                            .setTimeScale( TimeScale.of( Duration.ofDays( 1 ),
                                                                                         TimeScaleFunction.MAXIMUM ) )
                                                            .build();

        SampleMetadata metaTwo = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                            .build();

        SampleMetadata metaThree = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                              .setProjectConfig( new ProjectConfig( null,
                                                                                                    null,
                                                                                                    null,
                                                                                                    null,
                                                                                                    null,
                                                                                                    "A" ) )
                                                              .setTimeScale( TimeScale.of( Duration.ofDays( 1 ),
                                                                                           TimeScaleFunction.MAXIMUM ) )
                                                              .build();

        assertTrue( metaOne.equalsWithoutTimeWindowOrThresholds( metaOne ) );

        assertFalse( metaOne.equalsWithoutTimeWindowOrThresholds( metaTwo ) );

        assertFalse( metaOne.equalsWithoutTimeWindowOrThresholds( metaThree ) );

        SampleMetadata metaFour = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                             .setTimeScale( TimeScale.of( Duration.ofDays( 1 ),
                                                                                          TimeScaleFunction.MEAN ) )
                                                             .build();

        SampleMetadata metaFive = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                             .setTimeScale( TimeScale.of( Duration.ofDays( 1 ),
                                                                                          TimeScaleFunction.MAXIMUM ) )
                                                             .build();

        SampleMetadata metaSix = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                            .setProjectConfig( new ProjectConfig( null,
                                                                                                  null,
                                                                                                  null,
                                                                                                  null,
                                                                                                  null,
                                                                                                  "A" ) )
                                                            .build();

        assertFalse( metaFour.equalsWithoutTimeWindowOrThresholds( metaOne ) );

        assertTrue( metaFour.equalsWithoutTimeWindowOrThresholds( metaFour ) );

        assertFalse( metaFour.equalsWithoutTimeWindowOrThresholds( metaFive ) );

        assertFalse( metaSix.equalsWithoutTimeWindowOrThresholds( metaOne ) );
    }

    /**
     * Test {@link SampleMetadata#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertEquals( SampleMetadata.of().hashCode(), SampleMetadata.of().hashCode() );
        Location l1 = Location.of( DRRC2 );
        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( l1, SQIN, HEFS ) );
        assertEquals( m1.hashCode(), m1.hashCode() );
        Location l2 = Location.of( DRRC2 );
        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of(),
                                               DatasetIdentifier.of( l2, SQIN, HEFS ) );
        assertEquals( m1.hashCode(), m2.hashCode() );
        Location l3 = Location.of( DRRC2 );
        SampleMetadata m3 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( l3, SQIN, HEFS ) );
        Location l4 = Location.of( DRRC2 );
        SampleMetadata m4 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( l4, SQIN, HEFS ) );
        assertEquals( m3.hashCode(), m4.hashCode() );
        Location l4t = Location.of( DRRC2 );
        SampleMetadata m4t = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( l4t, SQIN, HEFS ) );
        assertEquals( m4.hashCode(), m4t.hashCode() );
        assertEquals( m3.hashCode(), m4t.hashCode() );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( m1.hashCode() == m2.hashCode() );
        }

        // Add a time window
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                Instant.parse( SECOND_TIME ) );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                 Instant.parse( SECOND_TIME ) );
        Location l6 = Location.of( DRRC3 );
        final TimeWindow timeWindow2 = firstWindow;
        SampleMetadata m6 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                       .setIdentifier( DatasetIdentifier.of( l6, SQIN, HEFS ) )
                                                       .setTimeWindow( timeWindow2 )
                                                       .build();
        Location l7 = Location.of( DRRC3 );
        final TimeWindow timeWindow3 = secondWindow;
        SampleMetadata m7 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                       .setIdentifier( DatasetIdentifier.of( l7, SQIN, HEFS ) )
                                                       .setTimeWindow( timeWindow3 )
                                                       .build();
        assertEquals( m6.hashCode(), m7.hashCode() );
        assertEquals( m7.hashCode(), m6.hashCode() );

        TimeWindow thirdWindow = TimeWindow.of( Instant.parse( FIRST_TIME ),
                                                Instant.parse( SECOND_TIME ),
                                                Instant.parse( FIRST_TIME ),
                                                Instant.parse( SECOND_TIME ) );
        Location l8 = Location.of( DRRC3 );

        // Add a threshold
        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        SampleMetadata m9 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                               DatasetIdentifier.of( l8, SQIN, HEFS ),
                                               thirdWindow,
                                               thresholds );

        SampleMetadata m10 = SampleMetadata.of( MeasurementUnit.of( TEST_DIMENSION ),
                                                DatasetIdentifier.of( l8, SQIN, HEFS ),
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
        final TimeWindow timeWindow = thirdWindow;
        final OneOrTwoThresholds thresholds1 = thresholds;
        final ProjectConfig projectConfig = mockConfigOne;

        SampleMetadata m11 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                        .setIdentifier( DatasetIdentifier.of( l8, SQIN, HEFS ) )
                                                        .setTimeWindow( timeWindow )
                                                        .setThresholds( thresholds1 )
                                                        .setProjectConfig( projectConfig )
                                                        .build();
        final TimeWindow timeWindow1 = thirdWindow;
        final OneOrTwoThresholds thresholds2 = thresholds;
        final ProjectConfig projectConfig1 = mockConfigTwo;

        SampleMetadata m12 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( TEST_DIMENSION ) )
                                                        .setIdentifier( DatasetIdentifier.of( l8, SQIN, HEFS ) )
                                                        .setTimeWindow( timeWindow1 )
                                                        .setThresholds( thresholds2 )
                                                        .setProjectConfig( projectConfig1 )
                                                        .build();

        assertEquals( m11.hashCode(), m12.hashCode() );
    }

    /**
     * Confirms that {@link SampleMetadata#toString()} produces an expected string.
     */

    @Test
    public void testToString()
    {
        // Simplest case
        assertEquals( "SampleMetadata[datasetIdentifier=<null>,timeWindow=<null>,"
                      + "thresholds=<null>,timeScale=<null>,measurementUnit=DIMENSIONLESS]",
                      SampleMetadata.of().toString() );

        // Most complex case
        DatasetIdentifier identifier = DatasetIdentifier.of( Location.of( "A" ), "B" );

        OneOrTwoThresholds thresholds = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                             Operator.EQUAL,
                                                                             ThresholdDataType.LEFT ) );

        TimeWindow timeWindow =
                TimeWindow.of( Instant.parse( THIRD_TIME ),
                               Instant.parse( THIRD_TIME ),
                               Instant.parse( THIRD_TIME ),
                               Instant.parse( THIRD_TIME ),
                               Duration.ZERO,
                               Duration.ZERO );

        SampleMetadata meta = new SampleMetadataBuilder().setFromExistingInstance( SampleMetadata.of() )
                                                         .setIdentifier( identifier )
                                                         .setMeasurementUnit( MeasurementUnit.of() )
                                                         .setThresholds( thresholds )
                                                         .setTimeWindow( timeWindow )
                                                         .setTimeScale( TimeScale.of( Duration.ofDays( 1 ),
                                                                                      TimeScaleFunction.MEAN ) )
                                                         .build();

        assertEquals( "SampleMetadata[datasetIdentifier=DatasetIdentifier[geospatialId=A,variableId=B,"
                      + "scenarioId=<null>,baselineScenarioId=<null>,pairContext=<null>],"
                      + "timeWindow=[2000-02-02T00:00:00Z,2000-02-02T00:00:00Z,2000-02-02T00:00:00Z,"
                      + "2000-02-02T00:00:00Z,PT0S,PT0S],thresholds== 1.0,timeScale=[PT24H,MEAN],"
                      + "measurementUnit=DIMENSIONLESS]",
                      meta.toString() );

    }

}
