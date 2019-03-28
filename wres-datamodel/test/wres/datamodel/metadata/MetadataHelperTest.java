package wres.datamodel.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.metadata.TimeScale.TimeScaleFunction;

/**
 * Tests the {@link MetadataHelper}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetadataHelperTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link MetadataHelper#unionOf(java.util.List)} against a benchmark.
     */
    @Test
    public void unionOf()
    {
        Location l1 = Location.of( "DRRC2" );
        SampleMetadata m1 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                       .setIdentifier( DatasetIdentifier.of( l1, "SQIN", "HEFS" ) )
                                                       .setTimeWindow( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                      Instant.parse( "1985-12-31T23:59:59Z" ) ) )
                                                       .build();
        Location l2 = Location.of( "DRRC2" );
        SampleMetadata m2 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                       .setIdentifier( DatasetIdentifier.of( l2, "SQIN", "HEFS" ) )
                                                       .setTimeWindow( TimeWindow.of( Instant.parse( "1986-01-01T00:00:00Z" ),
                                                                                      Instant.parse( "1986-12-31T23:59:59Z" ) ) )
                                                       .build();
        Location l3 = Location.of( "DRRC2" );
        SampleMetadata m3 = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                       .setIdentifier( DatasetIdentifier.of( l3, "SQIN", "HEFS" ) )
                                                       .setTimeWindow( TimeWindow.of( Instant.parse( "1987-01-01T00:00:00Z" ),
                                                                                      Instant.parse( "1988-01-01T00:00:00Z" ) ) )
                                                       .build();
        Location benchmarkLocation = Location.of( "DRRC2" );
        SampleMetadata benchmark = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of() )
                                                              .setIdentifier( DatasetIdentifier.of( benchmarkLocation,
                                                                                                    "SQIN",
                                                                                                    "HEFS" ) )
                                                              .setTimeWindow( TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                                                             Instant.parse( "1988-01-01T00:00:00Z" ) ) )
                                                              .build();

        assertEquals( "Unexpected difference between union of metadata and benchmark.",
                      benchmark,
                      MetadataHelper.unionOf( Arrays.asList( m1, m2, m3 ) ) );
    }

    /**
     * Tests that the {@link MetadataHelper#unionOf(java.util.List)} throws an expected exception when the input is
     * null.
     */
    @Test
    public void testUnionOfThrowsExceptionWithNullInput()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot find the union of null metadata." );

        MetadataHelper.unionOf( null );
    }

    /**
     * Tests that the {@link MetadataHelper#unionOf(java.util.List)} throws an expected exception when the input is
     * empty.
     */
    @Test
    public void testUnionOfThrowsExceptionWithEmptyInput()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot find the union of empty input." );

        MetadataHelper.unionOf( Collections.emptyList() );
    }

    /**
     * Tests that the {@link MetadataHelper#unionOf(java.util.List)} throws an expected exception when the input is
     * contains a null.
     */
    @Test
    public void testUnionOfThrowsExceptionWithOneNullInput()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot find the union of null metadata." );

        MetadataHelper.unionOf( Arrays.asList( (SampleMetadata) null ) );
    }


    /**
     * Tests that the {@link MetadataHelper#unionOf(java.util.List)} throws an expected exception when the inputs are
     * unequal on attributes that are expected to be equal.
     */
    @Test
    public void testUnionOfThrowsExceptionWithUnequalInputs()
    {
        exception.expect( MetadataException.class );
        exception.expectMessage( "Only the time window and thresholds can differ when finding the union of metadata." );

        SampleMetadata failOne = SampleMetadata.of( MeasurementUnit.of(),
                                                    DatasetIdentifier.of( Location.of( "DRRC3" ), "SQIN", "HEFS" ) );
        SampleMetadata failTwo =
                SampleMetadata.of( MeasurementUnit.of(), DatasetIdentifier.of( Location.of( "A" ), "B" ) );

        MetadataHelper.unionOf( Arrays.asList( failOne, failTwo ) );
    }

    /**
     * Tests the {@link MetadataHelper#isChangeOfScaleRequired(TimeScale, TimeScale)}.
     */

    @Test
    public void testIsChangeOfScaleRequired()
    {
        // Different periods: true
        assertTrue( MetadataHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ) ),
                                                            TimeScale.of( Duration.ofHours( 2 ) ) ) );

        // Different periods: true
        assertTrue( MetadataHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ),
                                                                          TimeScaleFunction.UNKNOWN ),
                                                            TimeScale.of( Duration.ofHours( 2 ),
                                                                          TimeScaleFunction.MEAN ) ) );

        // Different functions: true
        assertTrue( MetadataHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ),
                                                                          TimeScaleFunction.MEAN ),
                                                            TimeScale.of( Duration.ofHours( 1 ),
                                                                          TimeScaleFunction.TOTAL ) ) );

        // Different functions, but left function is UNKNOWN: false 
        assertFalse( MetadataHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ),
                                                                           TimeScaleFunction.UNKNOWN ),
                                                             TimeScale.of( Duration.ofHours( 1 ),
                                                                           TimeScaleFunction.TOTAL ) ) );

        // Both instantaneous: false
        assertFalse( MetadataHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofMillis( 1 ) ),
                                                             TimeScale.of( Duration.ofSeconds( 1 ) ) ) );

    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that is null.
     */

    @Test
    public void testThrowExceptionIfExistingTimeScaleIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The existing time scale cannot be null." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( null, null, null );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a desired time scale that is null.
     */

    @Test
    public void testThrowExceptionIfDesiredTimeScaleIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The desired time scale cannot be null." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofMillis( 1 ) ), null, null );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a time step that is null.
     */

    @Test
    public void testThrowExceptionIfTimeStepIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The time-step duration cannot be null." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofMillis( 1 ) ),
                                                               TimeScale.of( Duration.ofMillis( 1 ) ),
                                                               null );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a time step that is {@link Duration#ZERO}.
     */

    @Test
    public void testThrowExceptionIfTimeStepIsZero()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The period associated with the time-step cannot be zero." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ) ),
                                                               TimeScale.of( Duration.ofHours( 2 ) ),
                                                               Duration.ZERO );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a time step that is negative.
     */

    @Test
    public void testThrowExceptionIfTimeStepIsNegative()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The period associated with the time-step cannot be negative." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ) ),
                                                               TimeScale.of( Duration.ofHours( 2 ) ),
                                                               Duration.ofMillis( -1 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that is smaller than the desired time scale.
     */

    @Test
    public void testThrowExceptionIfDownscalingRequested()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Downscaling is not supported: the desired time scale cannot be smaller than "
                                 + "the existing time scale." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofMinutes( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofMillis( 1 ) );
    }

    /**
     * Checks for the absence of an exception when calling
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale whose period is an integer multiple of the period associated with the desired time scale.
     */

    @Test( expected = Test.None.class /* no exception expected */ )
    public void testDoNotThrowExceptionIfDesiredPeriodCommutesFromExistingPeriod()
    {
        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofSeconds( 60 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofMillis( 1 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale whose period is not an integer multiple of the period associated with the desired time 
     * scale.
     */

    public void testThrowExceptionIfDesiredPeriodDoesNotCommuteFromExistingPeriod()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The desired period must be an integer multiple of the existing period." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 2 ) ),
                                                               TimeScale.of( Duration.ofSeconds( 61 ) ),
                                                               Duration.ofMillis( 1 ) );

    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale whose period is different than the period associated with the desired time scale, but
     * the functions are different.
     */

    @Test
    public void testThrowExceptionIfPeriodsMatchAndFunctionsDiffer()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The periods associated with the existing and desired time scales are the same, "
                                 + "but the time scale functions are different [MEAN, MAXIMUM]. The function cannot "
                                 + "be changed without changing the period." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MAXIMUM ),
                                                               Duration.ofMillis( 1 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a data time-step that exceeds the period associated with the desired time scale.
     */

    @Test
    public void testThrowExceptionIfDataTimeStepExceedsDesiredPeriod()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Insufficient data for rescaling: the time-step of the data cannot be "
                                 + "greater than the desired time scale when rescaling is required [PT120H,PT60H]." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofHours( 60 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofHours( 120 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a data time-step that matches the period associated with the desired time scale and rescaling is required.
     */

    @Test
    public void testThrowExceptionIfDataTimeStepMatchesDesiredPeriodAndRescalingIsRequired()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Insufficient data for rescaling: the period associated with the desired "
                                 + "time scale matches the time-step of the data (PT60H)." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofHours( 60 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofHours( 60 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that is not an integer multiple of the desired time scale.
     */

    @Test
    public void testThrowExceptionIfDesiredPeriodDoesNotCommuteFromDataTimeStep()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The desired period must be an integer multiple of the data time-step." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 2 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofHours( 60 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofMillis( 7 ) );

    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, Duration)} with
     * a desired time scale whose function is unknown.
     */
    @Test
    public void testThrowExceptionIfDesiredFunctionIsUnknown()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The desired time scale function is '" + TimeScaleFunction.UNKNOWN
                                 + "': the "
                                 + "function must be known to conduct rescaling." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 60 ) ),
                                                               TimeScale.of( Duration.ofHours( 120 ) ),
                                                               Duration.ofHours( 1 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that represents a 1 hour mean, an expected time-scale that is a 6h accumulation and a 
     * time-step that is 6h.
     */

    @Test
    public void testThrowExceptionWhenForming6HAccumulationFrom1HMean()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Cannot accumulate values that are not already accumulations. The function associated "
                                 + "with the existing time scale must be a 'TOTAL', rather than a 'MEAN', or the "
                                 + "function associated with the desired time scale must be changed." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofHours( 6 ),
                                                                             TimeScaleFunction.TOTAL ),
                                                               Duration.ofHours( 6 ) );

    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that represents instantaneous data, an expected time-scale that is a 6h accumulation 
     * and a time-step that is 6h. This represents issue #45113.
     */

    @Test
    public void testThrowExceptionWhenAccumulatingInst()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Cannot accumulate instantaneous values. Change the existing time scale or "
                                 + "change the function associated with the desired time scale to "
                                 + "something other than a 'TOTAL'" );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofHours( 6 ),
                                                                             TimeScaleFunction.TOTAL ),
                                                               Duration.ofHours( 6 ) );

    }

    /**
     * Checks for the absence of an exception when calling
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a desired time scale whose function is a total and an existing time scale whose function is unknown.
     */

    @Test( expected = Test.None.class /* no exception expected */ )
    public void testDoNotThrowExceptionIfAccumulatingUnknown()
    {
        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ) ),
                                                               TimeScale.of( Duration.ofHours( 60 ),
                                                                             TimeScaleFunction.TOTAL ),
                                                               Duration.ofHours( 1 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that represents instantaneous data and the expected time-scale is a 1h mean and the
     * time-step is 1h. This represents issue #57315.
     */

    @Test
    public void testThrowExceptionWhenForming1HMeanFrom1HInst()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Insufficient data for rescaling: the period associated with the desired time "
                                 + "scale matches the time-step of the data (PT1H)." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofHours( 1 ) );

    }

    /**
     * Checks that no exception is thrown when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that equals the desired time scale.
     */

    @Test( expected = Test.None.class )
    public void testDoNotThrowExceptionWhenNoRescalingRequested()
    {
        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 120 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofSeconds( 120 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofMillis( 7 ) );

    }

}
