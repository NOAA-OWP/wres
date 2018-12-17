package wres.datamodel.metadata;

import static org.junit.Assert.assertEquals;

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
        exception.expect( MetadataException.class );
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
        exception.expect( MetadataException.class );
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
        exception.expect( MetadataException.class );
        exception.expectMessage( "Cannot find the union of null metadata." );

        MetadataHelper.unionOf( Arrays.asList( (SampleMetadata) null  ) );
    }

    
    /**
     * Tests that the {@link MetadataHelper#unionOf(java.util.List)} throws an expected exception when the inputs are
     * unequal on attributes that are expected to be equal.
     */
    @Test
    public void testUnionOfThrowsExceptionWithEmpty2Input()
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

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofMillis( 1 ) ),
                                                               Duration.ofMillis( 1 ) );
    }

    /**
     * Checks for the absence of an exception when calling
     * {@link MetadataHelper#throwExceptionIfChangeOfScaleIsInvalid(TimeScale, TimeScale, java.time.Duration)} with
     * an existing time scale whose period is an integer multiple of the period associated with the desired time scale.
     */

    @Test(expected = Test.None.class /* no exception expected */)
    public void doNotThrowExceptionIfDesiredPeriodCommutesFromExistingPeriod()
    {
        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofSeconds( 60 ) ),
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
                                 + "but the time scale functions are different. The function cannot be changed without "
                                 + "changing the period." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               TimeScale.of( Duration.ofSeconds( 1 ),
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

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ),
                                                                             TimeScaleFunction.MAXIMUM ),
                                                               TimeScale.of( Duration.ofSeconds( 10 ),
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
        exception.expectMessage( "Insufficient data for resclaing: the time-step of the data cannot be "
                + "greater than the desired time scale." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofSeconds( 60 ) ),
                                                               Duration.ofSeconds( 120 ) );
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
                + "time scale matches the time-step of the data." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofSeconds( 60 ) ),
                                                               Duration.ofSeconds( 60 ) );
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

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 2 ) ),
                                                               TimeScale.of( Duration.ofSeconds( 60 ) ),
                                                               Duration.ofMillis( 7 ) );

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
                                                               TimeScale.of( Duration.ofHours( 6 ), TimeScaleFunction.TOTAL ),
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
                + "scale matches the time-step of the data." );

        MetadataHelper.throwExceptionIfChangeOfScaleIsInvalid( TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                               TimeScale.of( Duration.ofHours( 1 ),
                                                                             TimeScaleFunction.MEAN ),
                                                               Duration.ofHours( 1 ) );
    
    }
    
}
