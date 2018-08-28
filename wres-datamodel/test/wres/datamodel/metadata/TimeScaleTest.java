package wres.datamodel.metadata;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.BeforeClass;
import org.junit.Test;

import wres.config.generated.DurationUnit;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.metadata.TimeScale.TimeScaleFunction;

/**
 * Tests the {@link TimeScale}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeScaleTest
{

    /**
     * Common instance for use in multiple tests.
     */

    private static TimeScale timeScale;

    @BeforeClass
    public static void setupBeforeAllTests()
    {
        timeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.AVG );
    }

    /**
     * Confirms that the {@link TimeScale#getPeriod()} returns an expected period.
     */

    @Test
    public void testGetPeriodReturnsExpectedPeriod()
    {
        assertEquals( timeScale.getPeriod(), Duration.ofDays( 1 ) );
    }

    /**
     * Confirms that the {@link TimeScale#getFunction()} returns an expected function.
     */

    @Test
    public void testGetFunctionReturnsExpectedFunction()
    {
        assertEquals( timeScale.getFunction(), TimeScaleFunction.AVG );
    }

    /**
     * Confirms that {@link TimeScale#of(wres.config.generated.TimeScaleConfig)} produces expected instances.
     */

    @Test
    public void testConstructionWithConfigProducesExpectedTimeScale()
    {
        TimeScaleConfig first =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.AVG, 1, null, DurationUnit.DAYS, null );

        assertEquals( TimeScale.of( first ), TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.AVG ) );

        TimeScaleConfig second =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.MAX, 1, null, DurationUnit.HOURS, null );

        assertEquals( TimeScale.of( second ), TimeScale.of( Duration.ofHours( 1 ), TimeScaleFunction.MAX ) );

        // Null function produces TimeScaleFunction.UNKNOWN
        TimeScaleConfig third =
                new TimeScaleConfig( null, 1, null, DurationUnit.SECONDS, null );

        assertEquals( TimeScale.of( third ), TimeScale.of( Duration.ofSeconds( 1 ), TimeScaleFunction.UNKNOWN ) );
    }

    /**
     * Tests the {@link TimeScale#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertEquals( timeScale, timeScale );

        // Symmetric
        TimeScale otherTimeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.AVG );
        assertEquals( timeScale, otherTimeScale );

        // Transitive, noting that timeScale and otherTimeScale are equal above
        TimeScale oneMoreTimeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.AVG );
        assertEquals( otherTimeScale, oneMoreTimeScale );
        assertEquals( timeScale, oneMoreTimeScale );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( timeScale.equals( otherTimeScale ) );
        }

        // Object unequal to null
        assertThat( timeScale, not( equalTo( null ) ) );

        // Unequal on period
        TimeScale unequalOnPeriod = TimeScale.of( Duration.ofDays( 3 ), TimeScaleFunction.AVG );
        assertThat( timeScale, not( equalTo( unequalOnPeriod ) ) );

        // Unequal on function
        TimeScale unequalOnFunction = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MIN );
        assertThat( timeScale, not( equalTo( unequalOnFunction ) ) );
    }

    /**
     * Tests the {@link TimeScale#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Consistent with equals 
        TimeScale otherTimeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.AVG );

        assertEquals( timeScale.hashCode(), otherTimeScale.hashCode() );

        // Repeatable within one execution context
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( timeScale.hashCode(), otherTimeScale.hashCode() );
        }
    }

    /**
     * Tests that the {@link TimeScale#toString()} produces an expected string representation.
     */
    @Test
    public void testToString()
    {
        assertEquals( timeScale.toString(), "[PT24H,AVG]" );
    }
}
