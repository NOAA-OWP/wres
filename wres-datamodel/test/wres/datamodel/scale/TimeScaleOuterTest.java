package wres.datamodel.scale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Tests the {@link TimeScaleOuter}.
 *
 * @author James Brown
 */
final class TimeScaleOuterTest
{

    /**
     * Common instance for use in multiple tests.
     */

    private TimeScaleOuter timeScale;

    @BeforeEach
    void setupBeforeEachTest()
    {
        this.timeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
    }

    /**
     * Confirms that the {@link TimeScaleOuter#getPeriod()} returns an expected period.
     */

    @Test
    void testGetPeriodReturnsExpectedPeriod()
    {
        assertEquals( this.timeScale.getPeriod(), Duration.ofDays( 1 ) );
    }

    /**
     * Confirms that the {@link TimeScaleOuter#getFunction()} returns an expected function.
     */

    @Test
    void testGetFunctionReturnsExpectedFunction()
    {
        assertEquals( TimeScaleFunction.MEAN, this.timeScale.getFunction() );

        assertEquals( TimeScaleFunction.UNKNOWN, TimeScaleOuter.of( Duration.ofSeconds( 1 ) ).getFunction() );
    }

    /**
     * Confirms that {@link TimeScaleOuter#of()} produces an expected instance.
     */

    @Test
    void testConstructionProducesExpectedTimeScale()
    {
        assertEquals( TimeScaleOuter.of(), TimeScaleOuter.of( Duration.ofMillis( 1 ), TimeScaleFunction.UNKNOWN ) );
    }

    /**
     * Tests the {@link TimeScaleOuter#equals(Object)}.
     */

    @Test
    void testEquals()
    {
        // Reflexive 
        assertEquals( this.timeScale, this.timeScale );

        // Symmetric
        TimeScaleOuter otherTimeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
        assertEquals( this.timeScale, otherTimeScale );

        // Transitive, noting that timeScale and otherTimeScale are equal above
        TimeScaleOuter oneMoreTimeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
        assertEquals( otherTimeScale, oneMoreTimeScale );
        assertEquals( this.timeScale, oneMoreTimeScale );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.timeScale, otherTimeScale );
        }

        // Object unequal to null
        assertNotEquals( null, this.timeScale );

        // Unequal on period
        TimeScaleOuter unequalOnPeriod = TimeScaleOuter.of( Duration.ofDays( 3 ), TimeScaleFunction.MEAN );
        assertNotEquals( this.timeScale, unequalOnPeriod );

        // Unequal on function
        TimeScaleOuter unequalOnFunction = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MINIMUM );
        assertNotEquals( this.timeScale, unequalOnFunction );
    }

    @Test
    void testEqualsOrInstantaneous()
    {
        // Reflexive 
        assertTrue( TimeScaleOuter.of().equalsOrInstantaneous( TimeScaleOuter.of() ) );

        TimeScaleOuter aTimeScale = TimeScaleOuter.of( Duration.ofSeconds( 1 ), TimeScaleFunction.MEAN );

        // Symmetric for instantaneous
        TimeScaleOuter otherTimeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.MEAN );
        assertTrue( aTimeScale.equalsOrInstantaneous( otherTimeScale ) );

        // Transitive, noting that timeScale and otherTimeScale are equal above
        TimeScaleOuter oneMoreTimeScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.MEAN );
        assertTrue( otherTimeScale.equalsOrInstantaneous( oneMoreTimeScale ) );
        assertTrue( aTimeScale.equalsOrInstantaneous( oneMoreTimeScale ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( aTimeScale.equalsOrInstantaneous( otherTimeScale ) );
        }

        // Object unequal to null
        assertFalse( aTimeScale.equalsOrInstantaneous( null ) );

        // Unequal on period
        TimeScaleOuter unequalOnPeriod = TimeScaleOuter.of( Duration.ofDays( 3 ), TimeScaleFunction.MEAN );
        assertFalse( aTimeScale.equalsOrInstantaneous( unequalOnPeriod ) );
    }

    /**
     * Tests the {@link TimeScaleOuter#hashCode()}.
     */

    @Test
    void testHashcode()
    {
        // Consistent with equals 
        TimeScaleOuter otherTimeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );

        assertEquals( this.timeScale.hashCode(), otherTimeScale.hashCode() );

        // Repeatable within one execution context
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.timeScale.hashCode(), otherTimeScale.hashCode() );
        }
    }

    /**
     * Tests that the {@link TimeScaleOuter#toString()} produces an expected string representation.
     */
    @Test
    void testToString()
    {
        assertEquals( "[PT24H,MEAN]", this.timeScale.toString() );

        assertEquals( "[INSTANTANEOUS]", TimeScaleOuter.of( Duration.ofSeconds( 1 ) ).toString() );
    }

    /**
     * Tests for an expected exception when attempting to build a {@link TimeScaleOuter} that has a zero period.
     */

    @Test
    void testConstructionThrowsExceptionWhenPeriodIsZero()
    {
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.of( Duration.ZERO ) );

        assertEquals( "Cannot build a time scale with a period of zero.", actual.getMessage() );
    }

    /**
     * Tests for an expected exception when attempting to build a {@link TimeScaleOuter} that has a negative period.
     */

    @Test
    void testConstructionThrowsExceptionWhenPeriodIsNegative()
    {
        Duration duration = Duration.ofSeconds( -100 );
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.of( duration ) );

        assertEquals( "Cannot build a time scale with a negative period.", actual.getMessage() );
    }

    /**
     * Tests {@link TimeScaleOuter#compareTo(TimeScaleOuter)}.
     */

    @Test
    void testCompareTo()
    {
        TimeScaleOuter scale = TimeScaleOuter.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MEAN );
        TimeScaleOuter largerScale = TimeScaleOuter.of( Duration.ofSeconds( 120 ), TimeScaleFunction.MEAN );
        TimeScaleOuter largestScale = TimeScaleOuter.of( Duration.ofSeconds( 180 ), TimeScaleFunction.MEAN );

        //Equal returns 0
        assertEquals( 0, scale.compareTo( TimeScaleOuter.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MEAN ) ) );

        //Transitive
        //x.compareTo(y) > 0
        assertTrue( largestScale.compareTo( largerScale ) > 0 );
        //y.compareTo(z) > 0
        assertTrue( largerScale.compareTo( scale ) > 0 );
        //x.compareTo(z) > 0
        assertTrue( largestScale.compareTo( scale ) > 0 );

        //Differences on period
        assertTrue( largestScale.compareTo( largerScale ) > 0 );

        //Differences on function, declaration order
        assertTrue( scale.compareTo( TimeScaleOuter.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MAXIMUM ) ) < 0 );
    }

    /**
     * Tests the {@link TimeScaleOuter#isInstantaneous()}.
     */

    @Test
    void testIsInstantaneous()
    {
        assertTrue( TimeScaleOuter.of( Duration.ofSeconds( 60 ) ).isInstantaneous() );

        assertTrue( TimeScaleOuter.of( Duration.ofSeconds( 59 ) ).isInstantaneous() );

        assertFalse( TimeScaleOuter.of( Duration.ofSeconds( 61 ) ).isInstantaneous() );

        assertFalse( TimeScaleOuter.of( Duration.ofSeconds( 60 ).plusNanos( 1 ) ).isInstantaneous() );
    }


    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) throws an expected exception 
     * when the input is empty.
     */
    @Test
    void testGetLeastCommonScaleThrowsExceptionWithEmptyInput()
    {
        Set<TimeScaleOuter> emptySet = Collections.emptySet();
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.getLeastCommonTimeScale( emptySet ) );

        assertEquals( "Cannot compute the Least Common Scale from empty input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) throws an expected exception 
     * when the input is null.
     */
    @Test
    void testGetLeastCommonScaleThrowsExceptionWithNullInput()
    {
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> TimeScaleOuter.getLeastCommonTimeScale( null ) );

        assertEquals( "Cannot compute the Least Common Scale from null input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) throws an expected exception 
     * when the input contains more than two different time scales.
     */
    @Test
    void testGetLeastCommonScaleThrowsExceptionWithMoreThanTwoTimeScales()
    {
        // Insertion ordered set to reflect declaration order of enum type
        Set<TimeScaleOuter> scales = new TreeSet<>();

        scales.add( TimeScaleOuter.of( Duration.ofHours( 1 ), TimeScaleFunction.MEAN ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 1 ), TimeScaleFunction.MAXIMUM ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 1 ), TimeScaleFunction.TOTAL ) );

        RescalingException actual = assertThrows( RescalingException.class,
                                                  () -> TimeScaleOuter.getLeastCommonTimeScale( scales ) );

        String expected = "Could not determine the Least Common Scale from the input. Expected input "
                          + "with only one scale function that does not correspond to an instantaneous "
                          + "time scale. Instead found ["
                          + TimeScaleFunction.MEAN
                          + ", "
                          + TimeScaleFunction.TOTAL
                          + ", "
                          + TimeScaleFunction.MAXIMUM
                          + "].";

        assertEquals( expected, actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) throws an expected exception 
     * when the input contains two different time scales and none are instantaneous.
     */
    @Test
    void testGetLeastCommonScaleThrowsExceptionWithTwoTimeScalesOfWhichNoneAreInstantaneous()
    {
        Set<TimeScaleOuter> scales = new TreeSet<>();

        scales.add( TimeScaleOuter.of( Duration.ofHours( 1 ), TimeScaleFunction.MEAN ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 1 ), TimeScaleFunction.MAXIMUM ) );

        RescalingException actual = assertThrows( RescalingException.class,
                                                  () -> TimeScaleOuter.getLeastCommonTimeScale( scales ) );

        String expected = "Could not determine the Least Common Scale from the input. Expected input "
                          + "with only one scale function that does not correspond to an instantaneous "
                          + "time scale. Instead found ["
                          + TimeScaleFunction.MEAN
                          + ", "
                          + TimeScaleFunction.MAXIMUM
                          + "].";

        assertEquals( expected, actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) throws an expected exception 
     * when the input contains a time scale that is the {@link Long#MAX_VALUE} in seconds.
     */
    @Test
    void testGetLeastCommonScaleThrowsExceptionWhenTimeScaleOverflowsLongSeconds()
    {
        Set<TimeScaleOuter> scales = new TreeSet<>();

        scales.add( TimeScaleOuter.of( Duration.ofSeconds( Long.MAX_VALUE ) ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 1 ) ) );

        RescalingException actual = assertThrows( RescalingException.class,
                                                  () -> TimeScaleOuter.getLeastCommonTimeScale( scales ) );

        assertEquals( "While attempting to compute the Least Common Duration from the input:", actual.getMessage() );

        assertTrue( actual.getCause() instanceof ArithmeticException );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) returns the single time scale
     * from an input with one time scale.
     */
    @Test
    void testGetLeastCommonScaleReturnsInputWhenInputHasOne()
    {
        TimeScaleOuter one = TimeScaleOuter.of( Duration.ofSeconds( 1 ) );

        assertEquals( one, TimeScaleOuter.getLeastCommonTimeScale( Collections.singleton( one ) ) );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) returns the single 
     * non-instantaneous time scale from the input that contains one non-instantaneous time scale.
     */
    @Test
    void testGetLeastCommonScaleReturnsNonInstantaneousInputWhenInputHasTwoAndOneIsInstantaneous()
    {
        Set<TimeScaleOuter> scales = new HashSet<>( 2 );
        TimeScaleOuter expected = TimeScaleOuter.of( Duration.ofSeconds( 61 ) );
        scales.add( TimeScaleOuter.of( Duration.ofSeconds( 1 ) ) );
        scales.add( expected );

        assertEquals( expected, TimeScaleOuter.getLeastCommonTimeScale( scales ) );
    }

    /**
     * <p>Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(Set) returns the expected
     * LCS from an input containing three different time scales. Takes the example of (8,9,21) from:</p>
     *
     * <p><a href="https://en.wikipedia.org/wiki/Least_common_multiple">...</a>
     */
    @Test
    void testGetLeastCommonScaleReturnsExpectedResultFromThreeInputs()
    {
        Set<TimeScaleOuter> scales = new HashSet<>( 3 );

        scales.add( TimeScaleOuter.of( Duration.ofHours( 8 ) ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 9 ) ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 21 ) ) );

        TimeScaleOuter expected = TimeScaleOuter.of( Duration.ofHours( 504 ) );

        assertEquals( expected, TimeScaleOuter.getLeastCommonTimeScale( scales ) );
    }

    /**
     * <p>Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(Set) returns the expected
     * LCS from an input containing three different time scales. Takes the example of (8,9,21) from:</p>
     *
     * <p><a href="https://en.wikipedia.org/wiki/Least_common_multiple">...</a>
     */
    @Test
    void testGetLeastCommonScaleReturnsExpectedResultFromTwoInputs()
    {
        Set<TimeScaleOuter> scales = new HashSet<>( 3 );

        scales.add( TimeScaleOuter.of( Duration.ofHours( 8 ) ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 9 ) ) );

        TimeScaleOuter expected = TimeScaleOuter.of( Duration.ofHours( 72 ) );

        assertEquals( expected, TimeScaleOuter.getLeastCommonTimeScale( scales ) );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonDuration(java.util.Set) throws an expected exception 
     * when the input is empty.
     */
    @Test
    void testGetLeastCommonDurationThrowsExceptionWithEmptyInput()
    {
        Set<Duration> emptySet = Collections.emptySet();
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.getLeastCommonDuration( emptySet ) );

        assertEquals( "Cannot compute the Least Common Duration from empty input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonDuration(java.util.Set) throws an expected exception 
     * when the input is null.
     */
    @Test
    void testGetLeastCommonDurationThrowsExceptionWithNullInput()
    {
        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> TimeScaleOuter.getLeastCommonDuration( null ) );

        assertEquals( "Cannot compute the Least Common Duration from null input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonDuration(java.util.Set) returns the single duration
     * from an input with one time duration.
     */
    @Test
    void testGetLeastCommonDurationReturnsInputWhenInputHasOne()
    {
        Duration one = Duration.ofSeconds( 1 );
        assertEquals( one, TimeScaleOuter.getLeastCommonDuration( Collections.singleton( one ) ) );
    }

    /**
     * <p>Tests that the {@link TimeScaleOuter#getLeastCommonDuration(Set) returns the expected
     * LCM from an input containing three different time durations. Takes the example of (8,9,21) from:</p>
     *
     * <p><a href="https://en.wikipedia.org/wiki/Least_common_multiple">...</a>
     */
    @Test
    void testGetLeastCommonDurationReturnsExpectedResultFromThreeInputs()
    {
        Set<Duration> durations = new HashSet<>( 3 );

        durations.add( Duration.ofHours( 8 ) );
        durations.add( Duration.ofHours( 9 ) );
        durations.add( Duration.ofHours( 21 ) );

        Duration expected = Duration.ofHours( 504 );

        assertEquals( expected, TimeScaleOuter.getLeastCommonDuration( durations ) );
    }

    @Test
    void testGetOrInferPeriodFromTimeScaleIsInstantaneous()
    {
        TimeScaleOuter outer = TimeScaleOuter.of( TimeScaleOuter.INSTANTANEOUS_DURATION );

        assertEquals( TimeScaleOuter.INSTANTANEOUS_DURATION, TimeScaleOuter.getOrInferPeriodFromTimeScale( outer ) );
    }

    @Test
    void testGetOrInferPeriodFromTimeScaleIsEqualToPeriod()
    {
        TimeScaleOuter outer = TimeScaleOuter.of( Duration.ofDays( 2 ) );

        assertEquals( Duration.ofDays( 2 ), TimeScaleOuter.getOrInferPeriodFromTimeScale( outer ) );
    }

    @Test
    void testGetOrInferPeriodFromTimeScaleIsEqualToLeapYearPeriod()
    {
        // #40178-135
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setStartDay( 1 )
                                       .setStartMonth( 1 )
                                       .setEndDay( 31 )
                                       .setEndMonth( 3 )
                                       .build();
        TimeScaleOuter outer = TimeScaleOuter.of( timeScale );

        assertEquals( Duration.ofDays( 91 ), TimeScaleOuter.getOrInferPeriodFromTimeScale( outer ) );
    }

    @Test
    void testRescalingIsNotRequiredForEqualScales()
    {
        TimeScaleOuter existing = TimeScaleOuter.of( Duration.ofDays( 1 ) );
        TimeScaleOuter desired = TimeScaleOuter.of( Duration.ofDays( 1 ) );

        assertFalse( TimeScaleOuter.isRescalingRequired( existing, desired ) );
    }

    @Test
    void testRescalingIsNotRequiredForEqualMagnitudeScalesWithUnknownFunction()
    {
        TimeScaleOuter existing = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.UNKNOWN );
        TimeScaleOuter desired = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MAXIMUM );

        assertFalse( TimeScaleOuter.isRescalingRequired( existing, desired ) );
    }

    @Test
    void testRescalingIsNotRequiredForInstantaneousScales()
    {
        TimeScaleOuter existing = TimeScaleOuter.of( Duration.ofSeconds( 1 ) );
        TimeScaleOuter desired = TimeScaleOuter.of( Duration.ofSeconds( 60 ) );

        assertFalse( TimeScaleOuter.isRescalingRequired( existing, desired ) );
    }

    @Test
    void testRescalingIsRequiredForEqualMagnitudeScalesWithDifferentFunctions()
    {
        TimeScaleOuter existing = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
        TimeScaleOuter desired = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MAXIMUM );

        assertTrue( TimeScaleOuter.isRescalingRequired( existing, desired ) );
    }

    @Test
    void testRescalingIsRequiredForScalesWithDifferentPeriods()
    {
        TimeScaleOuter existing = TimeScaleOuter.of( Duration.ofHours( 1 ) );
        TimeScaleOuter desired = TimeScaleOuter.of( Duration.ofHours( 2 ) );

        assertTrue( TimeScaleOuter.isRescalingRequired( existing, desired ) );
    }

    @Test
    void testRescalingIsRequiredForScalesWithDifferentMonthDays()
    {
        TimeScale existing = TimeScale.newBuilder()
                                      .setStartDay( 1 )
                                      .setStartMonth( 1 )
                                      .setEndDay( 31 )
                                      .setEndMonth( 3 )
                                      .build();
        TimeScaleOuter existingOuter = TimeScaleOuter.of( existing );
        TimeScale desired = TimeScale.newBuilder()
                                     .setStartDay( 2 )
                                     .setStartMonth( 2 )
                                     .setEndDay( 30 )
                                     .setEndMonth( 4 )
                                     .build();
        TimeScaleOuter desiredOuter = TimeScaleOuter.of( desired );

        assertTrue( TimeScaleOuter.isRescalingRequired( existingOuter, desiredOuter ) );
    }

    @Test
    void testRescalingIsRequiredForPeriodScaleAndMonthDayScale()
    {
        TimeScaleOuter existing = TimeScaleOuter.of( Duration.ofHours( 1 ) );
        TimeScale desiredInner = TimeScale.newBuilder()
                                          .setStartDay( 2 )
                                          .setStartMonth( 2 )
                                          .setEndDay( 30 )
                                          .setEndMonth( 4 )
                                          .build();
        TimeScaleOuter desired = TimeScaleOuter.of( desiredInner );

        assertTrue( TimeScaleOuter.isRescalingRequired( existing, desired ) );
    }
}
