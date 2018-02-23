package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import wres.datamodel.time.Event;

/**
 * Tests the default implementation of an {@link Event}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class EventTest
{

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
     * Constructs an {@link Event} and tests {@link Event#equals(Object)} against other instances.
     */

    @Test
    public void testEquals()
    {
        // Reflexive 
        Event<String> event = Event.of( Instant.parse( "1985-01-05T12:00:00Z" ), "someValue" );
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
                    "(1985-01-05T12:00:00Z,someValue)".equals( event.toString() ) );
    }

    /**
     * Constructs an {@link Event} and tests for exceptions.
     */

    @Test
    public void testExceptions()
    {
        try
        {
            Event.of( null, "someValue" );
            fail( "Expected an exception on building an event with a null time." );
        }
        catch ( NullPointerException e )
        {
        }
        try
        {
            Event.of( Instant.parse( "1985-01-01T00:00:00Z" ), null );
            fail( "Expected an exception on building an event with a null value." );
        }
        catch ( NullPointerException e )
        {
        }
    }

}
