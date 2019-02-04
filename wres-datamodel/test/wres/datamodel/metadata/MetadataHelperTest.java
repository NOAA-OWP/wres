package wres.datamodel.metadata;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.math3.exception.MathArithmeticException;
import org.hamcrest.CoreMatchers;
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
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that is null.
     */

    @Test
    public void throwExceptionIfExistingTimeScaleIsNull()
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
    public void throwExceptionIfDesiredTimeScaleIsNull()
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
    public void throwExceptionIfTimeStepIsNull()
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
    public void throwExceptionIfTimeStepIsZero()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The time-step duration cannot be zero for rescaling purposes." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofMillis( 1 ) ),
                                                               TimeScale.of( Duration.ofMillis( 1 ) ),
                                                               Duration.ZERO );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a time step that is negative.
     */

    @Test
    public void throwExceptionIfTimeStepIsNegative()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The time-step duration cannot be negative for rescaling purposes." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofMillis( 1 ) ),
                                                               TimeScale.of( Duration.ofMillis( 1 ) ),
                                                               Duration.ofMillis( -1 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that is smaller than the desired time scale.
     */

    @Test
    public void throwExceptionIfDownscalingRequested()
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
    public void doNotThrowExceptionIfDesiredPeriodCommutesFromExistingPeriod()
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

    public void throwExceptionIfDesiredPeriodDoesNotCommuteFromExistingPeriod()
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
    public void throwExceptionIfPeriodsMatchAndFunctionsDiffer()
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
     * a desired time scale whose function is a {@link TimeScaleFunction#TOTAL} and the existing time scale has a 
     * function that is not a {@link TimeScaleFunction#TOTAL}.
     */

    @Test
    public void throwExceptionIfAccumulatingNonAccumulations()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Cannot accumulate values that are not already accumulations. The "
                                 + "function associated with the existing time scale must be a '"
                                 + TimeScaleFunction.TOTAL
                                 + "' or the function associated with the desired time scale must "
                                 + "be changed." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MAXIMUM ),
                                                               TimeScale.of( Duration.ofHours( 10 ),
                                                                             TimeScaleFunction.TOTAL ),
                                                               Duration.ofMillis( 1 ) );
    }


    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * a data time-step that exceeds the period associated with the desired time scale.
     */

    @Test
    public void throwExceptionIfDataTimeStepExceedsDesiredPeriod()
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
    public void throwExceptionIfDataTimeStepMatchesDesiredPeriodAndRescalingIsRequired()
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
    public void throwExceptionIfDesiredPeriodDoesNotCommuteFromDataTimeStep()
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
    public void throwExceptionIfDesiredFunctionIsUnknown()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "The desired time scale function is '" + TimeScaleFunction.UNKNOWN
                                 + "': the "
                                 + "function must be a known function." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofHours( 60 ) ),
                                                               TimeScale.of( Duration.ofHours( 120 ) ),
                                                               Duration.ofHours( 1 ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that represents instantaneous data, an expected time-scale is a 6h accumulation and a 
     * time-step that is 6h. This represents issue #45113.
     */

    @Test
    public void throwExceptionWhenForming6HAccumulationFrom6HInst()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Cannot accumulate values that are not already accumulations. The function associated "
                                 + "with the existing time scale must be a 'TOTAL' or the function associated with the desired time "
                                 + "scale must be changed." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofHours( 6 ),
                                                                             TimeScaleFunction.TOTAL ),
                                                               Duration.ofHours( 6 ) );

    }

    /**
     * Checks for an expected exception when calling 
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale that represents instantaneous data and the expected time-scale is a 1h mean and the
     * time-step is 1h. This represents issue #57315.
     */

    @Test
    public void throwExceptionWhenForming1HMeanFrom1HInst()
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
    public void doNotThrowExceptionWhenNoRescalingRequested()
    {
        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 120 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofSeconds( 120 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofMillis( 7 ) );

    }

    /**
     * Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) throws an expected exception 
     * when the input is empty.
     */
    @Test
    public void testGetLCSInSecondsThrowsExceptionWithEmptyInput()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "Cannot compute the Least Common Scale from empty input." );

        MetadataHelper.getLeastCommonScaleInSeconds( Collections.emptySet() );
    }

    /**
     * Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) throws an expected exception 
     * when the input is null.
     */
    @Test
    public void testGetLCSInSecondsThrowsExceptionWithNullInput()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot compute the Least Common Scale from null input." );

        MetadataHelper.getLeastCommonScaleInSeconds( null );
    }

    /**
     * Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) throws an expected exception 
     * when the input contains more than two different time scales.
     */
    @Test
    public void testGetLCSInSecondsThrowsExceptionWithMoreThanTwoTimeScales()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Could not determine the Least Common Scale from the input. Expected input "
                                 + "with only one scale function that does not correspond to an instantaneous "
                                 + "time scale. Instead found ["
                                 + TimeScaleFunction.MEAN
                                 + ", "
                                 + TimeScaleFunction.MAXIMUM
                                 + ", "
                                 + TimeScaleFunction.TOTAL
                                 + "]." );

        // Insertion ordered set to reflect declaration order of enum type
        Set<TimeScale> scales = new TreeSet<>();

        scales.add( TimeScale.of( Duration.ofHours( 1 ), TimeScaleFunction.MEAN ) );
        scales.add( TimeScale.of( Duration.ofHours( 1 ), TimeScaleFunction.MAXIMUM ) );
        scales.add( TimeScale.of( Duration.ofHours( 1 ), TimeScaleFunction.TOTAL ) );

        MetadataHelper.getLeastCommonScaleInSeconds( scales );
    }

    /**
     * Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) throws an expected exception 
     * when the input contains two different time scales and none are instantaneous.
     */
    @Test
    public void testGetLCSInSecondsThrowsExceptionWithTwoTimeScalesOfWhichNoneAreInstantaneous()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "Could not determine the Least Common Scale from the input. Expected input "
                                 + "with only one scale function that does not correspond to an instantaneous "
                                 + "time scale. Instead found ["
                                 + TimeScaleFunction.MEAN
                                 + ", "
                                 + TimeScaleFunction.MAXIMUM
                                 + "]." );

        Set<TimeScale> scales = new TreeSet<>();

        scales.add( TimeScale.of( Duration.ofHours( 1 ), TimeScaleFunction.MEAN ) );
        scales.add( TimeScale.of( Duration.ofHours( 1 ), TimeScaleFunction.MAXIMUM ) );

        MetadataHelper.getLeastCommonScaleInSeconds( scales );
    }

    /**
     * Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) throws an expected exception 
     * when the input contains a time scale that is the {@link Long#MAX_VALUE} in seconds.
     */
    @Test
    public void testGetLCSInSecondsThrowsExceptionWhenTimeScaleOverflowsLongSeconds()
    {
        exception.expect( RescalingException.class );
        exception.expectMessage( "While attempting to compute the Least Common Scale from the input:" );
        exception.expectCause( CoreMatchers.isA( MathArithmeticException.class ) );

        Set<TimeScale> scales = new TreeSet<>();

        scales.add( TimeScale.of( Duration.ofSeconds( Long.MAX_VALUE ) ) );
        scales.add( TimeScale.of( Duration.ofHours( 1 ) ) );

        MetadataHelper.getLeastCommonScaleInSeconds( scales );
    }

    /**
     * Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) returns the single time scale
     * from an input with one time scale.
     */
    @Test
    public void testGetLCSReturnsInputWhenInputHasOne()
    {
        TimeScale one = TimeScale.of( Duration.ofSeconds( 1 ) );

        assertEquals( one, MetadataHelper.getLeastCommonScaleInSeconds( Collections.singleton( one ) ) );
    }

    /**
     * Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) returns the single 
     * non-instantaneous time scale from the input that contains one non-instantaneous time scale.
     */
    @Test
    public void testGetLCSReturnsNonInstantaneousInputWhenInputHasTwoAndOneIsInstantaneous()
    {
        Set<TimeScale> scales = new HashSet<>( 2 );
        TimeScale expected = TimeScale.of( Duration.ofSeconds( 61 ) );
        scales.add( TimeScale.of( Duration.ofSeconds( 1 ) ) );
        scales.add( expected );

        assertEquals( expected, MetadataHelper.getLeastCommonScaleInSeconds( scales ) );
    }

    /**
     * <p>Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) returns the expected
     * LCS from an input containing three different time scales. Takes the example of (8,9,21) from:</p>
     * 
     * <p>https://en.wikipedia.org/wiki/Least_common_multiple
     */
    @Test
    public void testGetLCSReturnsExpectedResultFromThreeInputs()
    {
        Set<TimeScale> scales = new HashSet<>( 3 );

        scales.add( TimeScale.of( Duration.ofHours( 8 ) ) );
        scales.add( TimeScale.of( Duration.ofHours( 9 ) ) );
        scales.add( TimeScale.of( Duration.ofHours( 21 ) ) );

        TimeScale expected = TimeScale.of( Duration.ofHours( 504 ) );

        assertEquals( expected, MetadataHelper.getLeastCommonScaleInSeconds( scales ) );
    }

    /**
     * <p>Tests that the {@link MetadataHelper#getLeastCommonScaleInSeconds(java.util.Set) returns the expected
     * LCS from an input containing three different time scales. Takes the example of (8,9,21) from:</p>
     * 
     * <p>https://en.wikipedia.org/wiki/Least_common_multiple
     */
    @Test
    public void testGetLCSReturnsExpectedResultFromTwoInputs()
    {
        Set<TimeScale> scales = new HashSet<>( 3 );

        scales.add( TimeScale.of( Duration.ofHours( 8 ) ) );
        scales.add( TimeScale.of( Duration.ofHours( 9 ) ) );

        TimeScale expected = TimeScale.of( Duration.ofHours( 72 ) );

        assertEquals( expected, MetadataHelper.getLeastCommonScaleInSeconds( scales ) );
    }

}
