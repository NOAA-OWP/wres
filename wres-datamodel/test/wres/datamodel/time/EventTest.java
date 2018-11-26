package wres.datamodel.time;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests the {@link Event}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class EventTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Constructs an {@link Event} and confirms that the {@link Event#getTime()} returns the expected result.
     */

    @Test
    public void testGetTime()
    {
        Event<String> event = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "The event has an unexpected time.",
                    Instant.parse( "1985-01-05T12:00:00Z" ).equals( event.getTime() ) );
    }

    /**
     * Constructs an {@link Event} and confirms that the {@link Event#getValue()} returns the expected result.
     */

    @Test
    public void testGetValue()
    {
        Event<String> event = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "The event has an unexpected time.", "someValue".equals( event.getValue() ) );
    }

    /**
     * Constructs an {@link Event} and confirms that the {@link Event#getReferenceTime()} returns the expected result.
     */

    @Test
    public void testGetReferenceTime()
    {
        Event<String> event = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "The event has an unexpected reference time.",
                    Instant.parse( "1985-01-05T12:00:00Z" ).equals( event.getReferenceTime() ) );

        Event<String> eventTwo = Event.of( Instant.parse( "1985-01-04T12:00:00Z" ),
                                           Instant.parse( "1985-01-05T12:00:00Z" ),
                                           "someValue" );
        assertTrue( "The event has an unexpected reference time.",
                    Instant.parse( "1985-01-04T12:00:00Z" ).equals( eventTwo.getReferenceTime() ) );

    }
    
    /**
     * Constructs an {@link Event} and confirms that the {@link Event#getDuration()} returns the expected result.
     */

    @Test
    public void testGetDuration()
    {
        Event<String> event = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "The event has an unexpected duration.",
                    Duration.ZERO.equals( event.getDuration() ) );

        Event<String> eventTwo = Event.of( Instant.parse( "1985-01-04T12:00:00Z" ),
                                           Instant.parse( "1985-01-05T12:00:00Z" ),
                                           "someValue" );
        
        assertTrue( "The event has an unexpected duration.",
                    Duration.ofDays( 1 ).equals( eventTwo.getDuration() ) );

    }    

    /**
     * Constructs an {@link Event} and tests {@link Event#equals(Object)} against other instances.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        Event<String> event = Event.of( null,
                                        Instant.parse( "1985-01-05T12:00:00Z" ),
                                        "someValue" );
        assertTrue( "The event does not meet the equals contract for reflexivity.", event.equals( event ) );
        // Symmetric
        Event<String> otherEvent = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "The event does not meet the equals contract for symmetry.",
                    event.equals( otherEvent ) && otherEvent.equals( event ) );
        // Transitive
        Event<String> oneMoreEvent = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "The event does not meet the equals contract for transitivity.",
                    event.equals( otherEvent ) && otherEvent.equals( oneMoreEvent ) && event.equals( oneMoreEvent ) );
        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The event does not meet the equals contract for consistency.", event.equals( otherEvent ) );
        }
        // Nullity
        assertFalse( "The event does not meet the equals contract for nullity.", event.equals( null ) );
        // Check unequal cases for time and value
        Event<String> unequalOnTime = Event.of( Instant.parse( "1985-01-06T12:00:00Z" ), "someValue" );
        assertFalse( "Expected the event to differ on time.", event.equals( unequalOnTime ) );
        Event<String> unequalOnValue = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "otherValue" );
        assertFalse( "Expected the event to differ on value.", event.equals( unequalOnValue ) );
        Event<String> unequalOnReferenceTime = Event.of( Instant.parse( "1985-01-04T11:00:00Z" ),
                                                         Instant.parse( "1985-01-05T12:00:00Z" ),
                                                         "someValue" );
        assertFalse( "Expected the event to differ on reference time.", event.equals( unequalOnReferenceTime ) );
    }

    /**
     * Constructs an {@link Event} and tests {@link Event#hashCode()} against the hashes of other instances.
     */

    @Test
    public void testHashCode()
    {
        // Consistent with equals
        Event<String> event = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        Event<String> otherEvent = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "The hashcode of the event is inconsistent with equals.",
                    event.equals( otherEvent ) && event.hashCode() == otherEvent.hashCode() );
        // Consistent when called repeatedly
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The event does not meet the hashcode contract for consistency.",
                        event.hashCode() == otherEvent.hashCode() );
        }
    }

    /**
     * Constructs an {@link Event} and tests {@link Event#toString()} against other instances.
     */

    @Test
    public void testToString()
    {
        Event<String> event = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
        assertTrue( "Unexpected string representation of an event.",
                    "(1985-01-05T12:00:00Z,1985-01-05T12:00:00Z,someValue)".equals( event.toString() ) );
    }

    /**
     * Tests {@link Event#compareTo(Event)} against other instances.
     */

    @Test
    public void testCompareTo()
    {
        Instant basisTime = Instant.parse( "1985-01-01T00:00:00Z" );

        Event<String> event = Event.of( basisTime, Instant.parse( "1985-01-01T00:00:00Z" ), "someValue" );
        Event<String> isEqual = Event.of( basisTime, Instant.parse( "1985-01-01T00:00:00Z" ), "someValue" );
        Event<String> isLess = Event.of( basisTime, Instant.parse( "1985-01-02T00:00:00Z" ), "someValue" );
        Event<String> isGreater = Event.of( basisTime, Instant.parse( "1984-12-31T00:00:00Z" ), "someValue" );

        assertTrue( event.compareTo( isEqual ) == 0 );
        assertTrue( event.compareTo( isLess ) < 0 );
        assertTrue( event.compareTo( isGreater ) > 0 );

        Instant validTime = Instant.parse( "1985-01-01T01:00:00Z" );

        Event<String> eventTwo = Event.of( Instant.parse( "1985-01-01T00:00:00Z" ), validTime, "someValue" );
        Event<String> isEqualTwo = Event.of( Instant.parse( "1985-01-01T00:00:00Z" ), validTime, "someValue" );
        Event<String> isLessTwo = Event.of( Instant.parse( "1985-01-02T00:00:00Z" ), validTime, "someValue" );
        Event<String> isGreaterTwo = Event.of( Instant.parse( "1984-12-31T00:00:00Z" ), validTime, "someValue" );

        assertTrue( eventTwo.compareTo( isEqualTwo ) == 0 );
        assertTrue( eventTwo.compareTo( isLessTwo ) < 0 );
        assertTrue( eventTwo.compareTo( isGreaterTwo ) > 0 );
    }

    /**
     * Constructs an {@link Event} and tests for an exception when the event time is null.
     */

    @Test
    public void testExceptionOnNullEventTime()
    {
        exception.expect( NullPointerException.class );

        Event.of( null, "someValue" );
    }

    /**
     * Constructs an {@link Event} and tests for an exception when the event is null.
     */

    @Test
    public void testExceptionOnNullEvent()
    {
        exception.expect( NullPointerException.class );

        Event.of( Instant.parse( "1985-01-01T00:00:00Z" ), null );
    }

    /**
     * Constructs an {@link Event} and tests for an exception when the input to {@link Event#compareTo(Event)} is null.
     */

    @Test
    public void testExceptionOnNullInputForComparison()
    {
        exception.expect( NullPointerException.class );

        Event.of( Instant.parse( "1985-01-01T00:00:00Z" ), "someEvent" ).compareTo( null );
    }

}
