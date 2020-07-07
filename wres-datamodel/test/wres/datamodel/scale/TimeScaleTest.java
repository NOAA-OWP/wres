package wres.datamodel.scale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.math3.exception.MathArithmeticException;
import org.junit.BeforeClass;
import org.junit.Test;

import wres.config.generated.DurationUnit;
import wres.config.generated.TimeScaleConfig;

import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;

/**
 * Tests the {@link TimeScaleOuter}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeScaleTest
{

    /**
     * Common instance for use in multiple tests.
     */

    private static TimeScaleOuter timeScale;

    @BeforeClass
    public static void setupBeforeAllTests()
    {
        timeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
    }

    /**
     * Confirms that the {@link TimeScaleOuter#getPeriod()} returns an expected period.
     */

    @Test
    public void testGetPeriodReturnsExpectedPeriod()
    {
        assertEquals( timeScale.getPeriod(), Duration.ofDays( 1 ) );
    }

    /**
     * Confirms that the {@link TimeScaleOuter#getFunction()} returns an expected function.
     */

    @Test
    public void testGetFunctionReturnsExpectedFunction()
    {
        assertEquals( TimeScaleFunction.MEAN, timeScale.getFunction() );

        assertEquals( TimeScaleFunction.UNKNOWN, TimeScaleOuter.of( Duration.ofSeconds( 1 ) ).getFunction() );
    }

    /**
     * Confirms that {@link TimeScaleOuter#of()} produces an expected instance.
     */

    @Test
    public void testConstructionProducesExpectedTimeScale()
    {
        assertEquals( TimeScaleOuter.of(), TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.UNKNOWN ) );
    }

    /**
     * Confirms that {@link TimeScaleOuter#of(wres.config.generated.TimeScaleConfig)} produces expected instances.
     */

    @Test
    public void testConstructionWithConfigProducesExpectedTimeScale()
    {
        TimeScaleConfig first =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.MEAN, 1, DurationUnit.DAYS, null );

        assertEquals( TimeScaleOuter.of( first ), TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ) );

        TimeScaleConfig second =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.MAXIMUM, 1, DurationUnit.HOURS, null );

        assertEquals( TimeScaleOuter.of( second ),
                      TimeScaleOuter.of( Duration.ofHours( 1 ), TimeScaleFunction.MAXIMUM ) );

        // Null function produces TimeScaleFunction.UNKNOWN
        TimeScaleConfig third =
                new TimeScaleConfig( null, 1, DurationUnit.SECONDS, null );

        assertEquals( TimeScaleOuter.of( third ),
                      TimeScaleOuter.of( Duration.ofSeconds( 1 ), TimeScaleFunction.UNKNOWN ) );
    }

    /**
     * Tests the {@link TimeScaleOuter#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertEquals( timeScale, timeScale );

        // Symmetric
        TimeScaleOuter otherTimeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
        assertEquals( timeScale, otherTimeScale );

        // Transitive, noting that timeScale and otherTimeScale are equal above
        TimeScaleOuter oneMoreTimeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
        assertEquals( otherTimeScale, oneMoreTimeScale );
        assertEquals( timeScale, oneMoreTimeScale );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( timeScale.equals( otherTimeScale ) );
        }

        // Object unequal to null
        assertNotEquals( timeScale, null );

        // Unequal on period
        TimeScaleOuter unequalOnPeriod = TimeScaleOuter.of( Duration.ofDays( 3 ), TimeScaleFunction.MEAN );
        assertNotEquals( timeScale, unequalOnPeriod );

        // Unequal on function
        TimeScaleOuter unequalOnFunction = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MINIMUM );
        assertNotEquals( timeScale, unequalOnFunction );
    }

    /**
     * Tests the {@link TimeScaleOuter#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Consistent with equals 
        TimeScaleOuter otherTimeScale = TimeScaleOuter.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );

        assertEquals( timeScale.hashCode(), otherTimeScale.hashCode() );

        // Repeatable within one execution context
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( timeScale.hashCode(), otherTimeScale.hashCode() );
        }
    }

    /**
     * Tests that the {@link TimeScaleOuter#toString()} produces an expected string representation.
     */
    @Test
    public void testToString()
    {
        assertEquals( "[PT24H,MEAN]", timeScale.toString() );

        assertEquals( "[INSTANTANEOUS]", TimeScaleOuter.of( Duration.ofSeconds( 1 ) ).toString() );
    }

    /**
     * Tests for an expected exception when attempting to build a {@link TimeScaleOuter} that has a zero period.
     */

    @Test
    public void testConstructionThrowsExceptionWhenPeriodIsZero()
    {
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.of( Duration.ZERO ) );

        assertEquals( "Cannot build a time scale with a period of zero.", actual.getMessage() );
    }

    /**
     * Tests for an expected exception when attempting to build a {@link TimeScaleOuter} that has a negative period.
     */

    @Test
    public void testConstructionThrowsExceptionWhenPeriodIsNegative()
    {
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.of( Duration.ofSeconds( -100 ) ) );

        assertEquals( "Cannot build a time scale with a negative period.", actual.getMessage() );
    }

    /**
     * Tests {@link TimeScaleOuter#compareTo(TimeScaleOuter)}.
     */

    @Test
    public void testCompareTo()
    {
        TimeScaleOuter scale = TimeScaleOuter.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MEAN );
        TimeScaleOuter largerScale = TimeScaleOuter.of( Duration.ofSeconds( 120 ), TimeScaleFunction.MEAN );
        TimeScaleOuter largestScale = TimeScaleOuter.of( Duration.ofSeconds( 180 ), TimeScaleFunction.MEAN );

        //Equal returns 0
        assertTrue( scale.compareTo( TimeScaleOuter.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MEAN ) ) == 0 );

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
    public void testIsInstantaneous()
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
    public void testGetLCSThrowsExceptionWithEmptyInput()
    {
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.getLeastCommonTimeScale( Collections.emptySet() ) );

        assertEquals( "Cannot compute the Least Common Scale from empty input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) throws an expected exception 
     * when the input is null.
     */
    @Test
    public void testGetLCSThrowsExceptionWithNullInput()
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
    public void testGetLCSThrowsExceptionWithMoreThanTwoTimeScales()
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
                          + TimeScaleFunction.MAXIMUM
                          + ", "
                          + TimeScaleFunction.TOTAL
                          + "].";

        assertEquals( expected, actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) throws an expected exception 
     * when the input contains two different time scales and none are instantaneous.
     */
    @Test
    public void testGetLCSThrowsExceptionWithTwoTimeScalesOfWhichNoneAreInstantaneous()
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
    public void testGetLCSThrowsExceptionWhenTimeScaleOverflowsLongSeconds()
    {
        Set<TimeScaleOuter> scales = new TreeSet<>();

        scales.add( TimeScaleOuter.of( Duration.ofSeconds( Long.MAX_VALUE ) ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 1 ) ) );

        RescalingException actual = assertThrows( RescalingException.class,
                                                  () -> TimeScaleOuter.getLeastCommonTimeScale( scales ) );

        assertEquals( "While attempting to compute the Least Common Duration from the input:", actual.getMessage() );

        assertTrue( actual.getCause() instanceof MathArithmeticException );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) returns the single time scale
     * from an input with one time scale.
     */
    @Test
    public void testGetLCSReturnsInputWhenInputHasOne()
    {
        TimeScaleOuter one = TimeScaleOuter.of( Duration.ofSeconds( 1 ) );

        assertEquals( one, TimeScaleOuter.getLeastCommonTimeScale( Collections.singleton( one ) ) );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) returns the single 
     * non-instantaneous time scale from the input that contains one non-instantaneous time scale.
     */
    @Test
    public void testGetLCSReturnsNonInstantaneousInputWhenInputHasTwoAndOneIsInstantaneous()
    {
        Set<TimeScaleOuter> scales = new HashSet<>( 2 );
        TimeScaleOuter expected = TimeScaleOuter.of( Duration.ofSeconds( 61 ) );
        scales.add( TimeScaleOuter.of( Duration.ofSeconds( 1 ) ) );
        scales.add( expected );

        assertEquals( expected, TimeScaleOuter.getLeastCommonTimeScale( scales ) );
    }

    /**
     * <p>Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) returns the expected
     * LCS from an input containing three different time scales. Takes the example of (8,9,21) from:</p>
     * 
     * <p>https://en.wikipedia.org/wiki/Least_common_multiple
     */
    @Test
    public void testGetLCSReturnsExpectedResultFromThreeInputs()
    {
        Set<TimeScaleOuter> scales = new HashSet<>( 3 );

        scales.add( TimeScaleOuter.of( Duration.ofHours( 8 ) ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 9 ) ) );
        scales.add( TimeScaleOuter.of( Duration.ofHours( 21 ) ) );

        TimeScaleOuter expected = TimeScaleOuter.of( Duration.ofHours( 504 ) );

        assertEquals( expected, TimeScaleOuter.getLeastCommonTimeScale( scales ) );
    }

    /**
     * <p>Tests that the {@link TimeScaleOuter#getLeastCommonTimeScale(java.util.Set) returns the expected
     * LCS from an input containing three different time scales. Takes the example of (8,9,21) from:</p>
     * 
     * <p>https://en.wikipedia.org/wiki/Least_common_multiple
     */
    @Test
    public void testGetLCSReturnsExpectedResultFromTwoInputs()
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
    public void testGetLCDThrowsExceptionWithEmptyInput()
    {
        IllegalArgumentException actual = assertThrows( IllegalArgumentException.class,
                                                        () -> TimeScaleOuter.getLeastCommonDuration( Collections.emptySet() ) );

        assertEquals( "Cannot compute the Least Common Duration from empty input.", actual.getMessage() );
    }

    /**
     * Tests that the {@link TimeScaleOuter#getLeastCommonDuration(java.util.Set) throws an expected exception 
     * when the input is null.
     */
    @Test
    public void testGetLCDThrowsExceptionWithNullInput()
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
    public void testGetLCDReturnsInputWhenInputHasOne()
    {
        Duration one = Duration.ofSeconds( 1 );
        assertEquals( one, TimeScaleOuter.getLeastCommonDuration( Collections.singleton( one ) ) );
    }

    /**
     * <p>Tests that the {@link TimeScaleOuter#getLeastCommonDuration(java.util.Set) returns the expected
     * LCM from an input containing three different time durations. Takes the example of (8,9,21) from:</p>
     * 
     * <p>https://en.wikipedia.org/wiki/Least_common_multiple
     */
    @Test
    public void testGetLCDReturnsExpectedResultFromThreeInputs()
    {
        Set<Duration> durations = new HashSet<>( 3 );

        durations.add( Duration.ofHours( 8 ) );
        durations.add( Duration.ofHours( 9 ) );
        durations.add( Duration.ofHours( 21 ) );

        Duration expected = Duration.ofHours( 504 );

        assertEquals( expected, TimeScaleOuter.getLeastCommonDuration( durations ) );
    }

}
