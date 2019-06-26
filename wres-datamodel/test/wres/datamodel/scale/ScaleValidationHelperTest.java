package wres.datamodel.scale;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.scale.ScaleValidationEvent.EventType;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;

/**
 * Tests the {@link ScaleValidationHelper}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ScaleValidationHelperTest
{

    /**
     * Start of message
     */

    private static final String WHILE_VALIDATING_A_LEFT_DATA_SOURCE = "While validating a LEFT data source: ";

    /**
     * Data source identifier.
     */

    private static final String LEFT = "LEFT";

    /**
     * Empty data source configuration for testing.
     */

    private static final DataSourceConfig EMPTY_DATA_SOURCE_CONFIG =
            new DataSourceConfig( null, null, null, null, null, null, null, null, null );

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link ScaleValidationHelper#isChangeOfScaleRequired(TimeScale, TimeScale)}.
     */

    @Test
    public void testIsChangeOfScaleRequired()
    {
        // Different periods: true
        assertTrue( ScaleValidationHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ) ),
                                                                   TimeScale.of( Duration.ofHours( 2 ) ) ) );

        // Different periods: true
        assertTrue( ScaleValidationHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ),
                                                                                 TimeScaleFunction.UNKNOWN ),
                                                                   TimeScale.of( Duration.ofHours( 2 ),
                                                                                 TimeScaleFunction.MEAN ) ) );

        // Different functions: true
        assertTrue( ScaleValidationHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ),
                                                                                 TimeScaleFunction.MEAN ),
                                                                   TimeScale.of( Duration.ofHours( 1 ),
                                                                                 TimeScaleFunction.TOTAL ) ) );

        // Different functions, but left function is UNKNOWN: false 
        assertFalse( ScaleValidationHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofHours( 1 ),
                                                                                  TimeScaleFunction.UNKNOWN ),
                                                                    TimeScale.of( Duration.ofHours( 1 ),
                                                                                  TimeScaleFunction.TOTAL ) ) );

        // Both instantaneous: false
        assertFalse( ScaleValidationHelper.isChangeOfScaleRequired( TimeScale.of( Duration.ofMillis( 1 ) ),
                                                                    TimeScale.of( Duration.ofSeconds( 1 ) ) ) );

    }

    /**
     * Tests the {@link ScaleValidationHelper#hasEvent(List, EventType)}.
     */

    @Test
    public void testHasEvent()
    {
        List<ScaleValidationEvent> events = Arrays.asList( ScaleValidationEvent.error( "A" ),
                                                           ScaleValidationEvent.warn( "B" ),
                                                           ScaleValidationEvent.pass( "C" ) );

        assertTrue( ScaleValidationHelper.hasEvent( events, EventType.ERROR ) );

        assertTrue( ScaleValidationHelper.hasEvent( events, EventType.WARN ) );

        assertTrue( ScaleValidationHelper.hasEvent( events, EventType.PASS ) );

        assertFalse( ScaleValidationHelper.hasEvent( Collections.emptyList(), EventType.PASS ) );
    }

    @Test
    public void testHasEventThrowsNPEOnNullEvents()
    {
        exception.expect( NullPointerException.class );

        ScaleValidationHelper.hasEvent( null, EventType.ERROR );
    }

    @Test
    public void testHasEventThrowsNPEOnNullEventType()
    {
        exception.expect( NullPointerException.class );

        ScaleValidationHelper.hasEvent( Collections.emptyList(), null );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale that is null.
     */

    @Test
    public void testValidationFailsIfExistingTimeScaleIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The existing time scale cannot be null." );

        ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG, null, null, null, null );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a desired time scale that is null.
     */

    @Test
    public void testValidationFailsIfDesiredTimeScaleIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The desired time scale cannot be null." );

        ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                        TimeScale.of( Duration.ofMillis( 1 ) ),
                                                        null,
                                                        null,
                                                        null );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a time-step that is null.
     */

    @Test
    public void testValidationFailsIfTimeStepIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The time-step duration cannot be null." );

        ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                        TimeScale.of( Duration.ofMillis( 1 ) ),
                                                        TimeScale.of( Duration.ofMillis( 1 ) ),
                                                        null,
                                                        null );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a data source identifier that is null.
     */

    @Test
    public void testValidationFailsIfDataSourceIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "The data source identifier cannot be null." );

        ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                        TimeScale.of( Duration.ofMillis( 1 ) ),
                                                        TimeScale.of( Duration.ofMillis( 1 ) ),
                                                        Duration.ofHours( 1 ),
                                                        null );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a time-step that is {@link Duration#ZERO}.
     */

    @Test
    public void testValidationFailsIfTimeStepIsZero()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ) ),
                                                                TimeScale.of( Duration.ofHours( 2 ) ),
                                                                Duration.ZERO,
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( "While validating a LEFT data source: The period "
                                                                    + "associated with the time-step cannot be zero." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a time-step that is negative.
     */

    @Test
    public void testValidationFailsIfTimeStepIsNegative()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ) ),
                                                                TimeScale.of( Duration.ofHours( 2 ) ),
                                                                Duration.ofMillis( -1 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( "While validating a LEFT data source: The period "
                                                                    + "associated with the time-step cannot be "
                                                                    + "negative." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale that is smaller than the desired time scale.
     */

    @Test
    public void testValidationFailsIfDownscalingRequested()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofMinutes( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofMillis( 1 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "Downscaling is not supported: the desired time "
                                                                    + "scale of 'PT1M' cannot be smaller than the "
                                                                    + "existing time scale of 'PT1H'." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that validation succeeds when calling
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale whose period is an integer multiple of the period associated with the desired time 
     * scale.
     */

    @Test
    public void testValidationPassesIfDesiredPeriodCommutesFromExistingPeriod()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofSeconds( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofSeconds( 60 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofMillis( 1 ),
                                                                LEFT );

        assertFalse( ScaleValidationHelper.hasEvent( events, EventType.ERROR ) );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale whose period is not an integer multiple of the period associated with the desired 
     * time scale.
     */

    public void testValidationFailsIfDesiredPeriodDoesNotCommuteFromExistingPeriod()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofSeconds( 2 ) ),
                                                                TimeScale.of( Duration.ofSeconds( 61 ) ),
                                                                Duration.ofMillis( 1 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "The desired period must be an integer multiple of "
                                                                    + "the existing period." );

        assertTrue( events.contains( expected ) );

    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale whose period is different than the period associated with the desired time scale, 
     * but the functions are different.
     */

    @Test
    public void testValidationFailsIfPeriodsMatchAndFunctionsDiffer()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofHours( 1 ),
                                                                              TimeScaleFunction.MAXIMUM ),
                                                                Duration.ofMillis( 1 ),
                                                                LEFT );

        ScaleValidationEvent expected =
                ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                            + "The period associated with the existing and desired "
                                            + "time scales is 'PT1H', but the time scale function "
                                            + "associated with the existing time scale is 'MEAN', which "
                                            + "differs from the function associated with the desired time "
                                            + "scale, namely 'MAXIMUM'. This is not allowed. The function "
                                            + "cannot be changed without changing the period." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a data time-step that exceeds the period associated with the desired time scale.
     */

    @Test
    public void testValidationFailsIfDataTimeStepExceedsDesiredPeriod()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofHours( 60 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofHours( 120 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "Insufficient data for rescaling: The time-step "
                                                                    + "of the data is 'PT120H' and the period "
                                                                    + "associated with the desired time scale is "
                                                                    + "'PT60H'. The time-step of the data cannot be "
                                                                    + "greater than the desired time scale when "
                                                                    + "rescaling is required." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a data time-step that matches the period associated with the desired time scale and rescaling is required.
     */

    @Test
    public void testValidationFailsIfDataTimeStepMatchesDesiredPeriodAndRescalingIsRequired()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofHours( 60 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofHours( 60 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "Insufficient data for rescaling: the period "
                                                                    + "associated with the desired time scale matches "
                                                                    + "the time-step of the data (PT60H)." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale that is not an integer multiple of the desired time scale.
     */

    @Test
    public void testValidationFailsIfDesiredPeriodDoesNotCommuteFromDataTimeStep()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 2 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofHours( 60 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofMillis( 7 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "The desired period of 'PT60H' is not an integer "
                                                                    + "multiple of the data time-step, which is "
                                                                    + "'PT0.007S'. If the data has multiple time-steps "
                                                                    + "that vary by time or feature, it may not be "
                                                                    + "possible to achieve the desired time scale for "
                                                                    + "all of the data. In that case, consider removing "
                                                                    + "the desired time scale and performing an "
                                                                    + "evaluation at the existing time scale of the "
                                                                    + "data, where possible." );

        assertTrue( events.contains( expected ) );

    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with a desired time scale whose function is unknown.
     */
    @Test
    public void testValidationFailsIfDesiredFunctionIsUnknown()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 60 ) ),
                                                                TimeScale.of( Duration.ofHours( 120 ) ),
                                                                Duration.ofHours( 1 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "The desired time scale function is 'UNKNOWN': the "
                                                                    + "function must be known to conduct rescaling." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale that represents a 1 hour mean, an expected time-scale that is a 6h accumulation and 
     * a time-step that is 6h.
     */

    @Test
    public void testValidationFailsWhenForming6HAccumulationFrom1HMean()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofHours( 6 ),
                                                                              TimeScaleFunction.TOTAL ),
                                                                Duration.ofHours( 6 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "Cannot accumulate values that are not already "
                                                                    + "accumulations. The function associated "
                                                                    + "with the existing time scale must be a 'TOTAL', "
                                                                    + "rather than a 'MEAN', or the function "
                                                                    + "associated with the desired time scale must "
                                                                    + "be changed." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks for an expected exception when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)}
     * with an existing time scale that represents instantaneous data, an expected time-scale that is a 6h accumulation 
     * and a time-step that is 6h. This represents issue #45113.
     */

    @Test
    public void testValidationFailsWhenAccumulatingInst()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                                TimeScale.of( Duration.ofHours( 6 ),
                                                                              TimeScaleFunction.TOTAL ),
                                                                Duration.ofHours( 6 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "Cannot accumulate instantaneous values. "
                                                                    + "Change the existing time scale or "
                                                                    + "change the function associated with "
                                                                    + "the desired time scale to "
                                                                    + "something other than a 'TOTAL'." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that the validation passes when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)}
     * with an existing time scale that has unknown function and must be accumulated.
     */

    @Test
    public void testValidationPassesWhenAccumulatingUnknown()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofHours( 1 ) ),
                                                                TimeScale.of( Duration.ofHours( 60 ),
                                                                              TimeScaleFunction.TOTAL ),
                                                                Duration.ofHours( 1 ),
                                                                LEFT );

        assertFalse( ScaleValidationHelper.hasEvent( events, EventType.ERROR ) );
    }

    /**
     * Checks that the validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)}
     * with an existing time scale that represents instantaneous data and the expected time-scale is a 1h mean and the
     * time-step is 1h. This represents issue #57315.
     */

    @Test
    public void testValidationFailsWhenForming1HMeanFrom1HInst()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofSeconds( 1 ) ),
                                                                TimeScale.of( Duration.ofHours( 1 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofHours( 1 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "Insufficient data for rescaling: the period "
                                                                    + "associated with the desired time scale matches "
                                                                    + "the time-step of the data (PT1H)." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that the validation passes when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale that equals the desired time scale.
     */

    @Test
    public void testValidationPassesWhenNoRescalingRequested()
    {
        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( EMPTY_DATA_SOURCE_CONFIG,
                                                                TimeScale.of( Duration.ofSeconds( 120 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofSeconds( 120 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofMillis( 7 ),
                                                                LEFT );

        assertFalse( ScaleValidationHelper.hasEvent( events, EventType.ERROR ) );
    }

    /**
     * Checks that the validation produces a warning when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale that is inconsistent with the declaration, but both are instantaneous.
     */

    @Test
    public void testValidationPassesWithWarningWhenExistingTimeScalesDifferButBothAreInstanteneous()
    {
        TimeScaleConfig timeScaleConfig =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.MEAN, 59, DurationUnit.SECONDS, null );

        DataSourceConfig config =
                new DataSourceConfig( null, null, null, null, null, null, null, timeScaleConfig, null );

        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( config,
                                                                TimeScale.of( Duration.ofSeconds( 60 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofSeconds( 120 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofSeconds( 30 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.warn( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                   + "The existing time scale in the project "
                                                                   + "declaration is [PT59S,MEAN] and the existing "
                                                                   + "time scale associated with the data is "
                                                                   + "[PT1M,MEAN]. This discrepancy is allowed "
                                                                   + "because both are recognized by the system as "
                                                                   + "'INSTANTANEOUS'." );

        assertTrue( events.contains( expected ) );
    }

    /**
     * Checks that the validation fails when calling 
     * {@link ScaleValidationHelper#validateScaleInformation(DataSourceConfig, TimeScale, TimeScale, Duration, String)} 
     * with an existing time scale that is inconsistent with the declaration.
     */

    @Test
    public void testValidationFailsWhenExistingTimeScalesDiffer()
    {
        TimeScaleConfig timeScaleConfig =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.MEAN, 59, DurationUnit.HOURS, null );

        DataSourceConfig config =
                new DataSourceConfig( null, null, null, null, null, null, null, timeScaleConfig, null );

        List<ScaleValidationEvent> events =
                ScaleValidationHelper.validateScaleInformation( config,
                                                                TimeScale.of( Duration.ofHours( 60 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                TimeScale.of( Duration.ofHours( 120 ),
                                                                              TimeScaleFunction.MEAN ),
                                                                Duration.ofHours( 30 ),
                                                                LEFT );

        ScaleValidationEvent expected = ScaleValidationEvent.error( WHILE_VALIDATING_A_LEFT_DATA_SOURCE
                                                                    + "The existing time scale in the project "
                                                                    + "declaration is [PT59H,MEAN] and the existing "
                                                                    + "time scale associated with the data is "
                                                                    + "[PT60H,MEAN]. This inconsistency is not allowed. "
                                                                    + "Fix the declaration of the source." );

        assertTrue( events.contains( expected ) );
    }


}
