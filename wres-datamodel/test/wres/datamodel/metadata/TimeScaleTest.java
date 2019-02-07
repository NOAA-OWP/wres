package wres.datamodel.metadata;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.time.Duration;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Common instance for use in multiple tests.
     */

    private static TimeScale timeScale;

    @BeforeClass
    public static void setupBeforeAllTests()
    {
        timeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
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
        assertEquals( TimeScaleFunction.MEAN, timeScale.getFunction() );
        
        assertEquals( TimeScaleFunction.UNKNOWN, TimeScale.of( Duration.ofSeconds( 1 ) ).getFunction() );        
    }

    /**
     * Confirms that {@link TimeScale#of(wres.config.generated.TimeScaleConfig)} produces expected instances.
     */

    @Test
    public void testConstructionWithConfigProducesExpectedTimeScale()
    {
        TimeScaleConfig first =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.MEAN, 1, null, DurationUnit.DAYS, null );

        assertEquals( TimeScale.of( first ), TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN ) );

        TimeScaleConfig second =
                new TimeScaleConfig( wres.config.generated.TimeScaleFunction.MAXIMUM, 1, null, DurationUnit.HOURS, null );

        assertEquals( TimeScale.of( second ), TimeScale.of( Duration.ofHours( 1 ), TimeScaleFunction.MAXIMUM ) );

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
        TimeScale otherTimeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
        assertEquals( timeScale, otherTimeScale );

        // Transitive, noting that timeScale and otherTimeScale are equal above
        TimeScale oneMoreTimeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );
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
        TimeScale unequalOnPeriod = TimeScale.of( Duration.ofDays( 3 ), TimeScaleFunction.MEAN );
        assertThat( timeScale, not( equalTo( unequalOnPeriod ) ) );

        // Unequal on function
        TimeScale unequalOnFunction = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MINIMUM );
        assertThat( timeScale, not( equalTo( unequalOnFunction ) ) );
    }

    /**
     * Tests the {@link TimeScale#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Consistent with equals 
        TimeScale otherTimeScale = TimeScale.of( Duration.ofDays( 1 ), TimeScaleFunction.MEAN );

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
        assertEquals( "[PT24H,MEAN]", timeScale.toString() );

        assertEquals( "[INSTANTANEOUS]", TimeScale.of( Duration.ofSeconds( 1 ) ).toString() );
    }
    
    /**
     * Tests for an expected exception when attempting to build a {@link TimeScale} that has a zero period.
     */
    
    @Test
    public void testConstructionThrowsExceptionWhenPeriodIsZero()
    {
        exception.expect( IllegalArgumentException.class );
        
        exception.expectMessage( "Cannot build a time scale with a period of zero." );
        
        TimeScale.of( Duration.ZERO );
    }
    
    /**
     * Tests for an expected exception when attempting to build a {@link TimeScale} that has a negative period.
     */
    
    @Test
    public void testConstructionThrowsExceptionWhenPeriodIsNegative()
    {
        exception.expect( IllegalArgumentException.class );
        
        exception.expectMessage( "Cannot build a time scale with a negative period." );
        
        TimeScale.of( Duration.ofSeconds( -100 ) );
    }
    
    /**
     * Tests {@link TimeScale#compareTo(TimeScale)}.
     */

    @Test
    public void testCompareTo()
    {
        TimeScale scale = TimeScale.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MEAN );
        TimeScale largerScale = TimeScale.of( Duration.ofSeconds( 120 ), TimeScaleFunction.MEAN );
        TimeScale largestScale = TimeScale.of( Duration.ofSeconds( 180 ), TimeScaleFunction.MEAN );
        
        //Equal returns 0
        assertTrue( scale.compareTo( TimeScale.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MEAN ) ) == 0 );

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
        assertTrue( scale.compareTo( TimeScale.of( Duration.ofSeconds( 60 ), TimeScaleFunction.MAXIMUM ) ) < 0 );
    }
    
    /**
     * Tests the {@link TimeScale#isInstantaneous()}.
     */
    
    @Test
    public void testIsInstantaneous()
    {
        assertTrue( TimeScale.of( Duration.ofSeconds( 60 ) ).isInstantaneous() );
        
        assertTrue( TimeScale.of( Duration.ofSeconds( 59 ) ).isInstantaneous() );
        
        assertFalse( TimeScale.of( Duration.ofSeconds( 61 ) ).isInstantaneous() );
        
        assertFalse( TimeScale.of( Duration.ofSeconds( 60 ).plusNanos( 1 ) ).isInstantaneous() );
    }
    
}
