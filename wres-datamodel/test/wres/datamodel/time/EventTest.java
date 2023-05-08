package wres.datamodel.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Test;

/**
 * Tests the {@link Event}.
 * 
 * @author James Brown
 */
public final class EventTest
{

    private static final String EVENT_VALUE = "someValue";
    private static final String FIFTH_TIME = "1985-01-05T12:00:00Z";
    private static final String FOURTH_TIME = "1985-01-04T12:00:00Z";
    private static final String THIRD_TIME = "1984-12-31T00:00:00Z";
    private static final String SECOND_TIME = "1985-01-02T00:00:00Z";
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    @Test
    public void testGetTime()
    {
        Event<String> event = Event.of( Instant.parse( FIFTH_TIME ), EVENT_VALUE );
        assertEquals( "The event has an unexpected time.", Instant.parse( FIFTH_TIME ), event.getTime() );
    }

    @Test
    public void testGetValue()
    {
        Event<String> event = Event.of( Instant.parse( FIFTH_TIME ), EVENT_VALUE );
        assertEquals( "The event has an unexpected time.", EVENT_VALUE, event.getValue() );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        Event<String> event = Event.of( Instant.parse( FIFTH_TIME ),
                                        EVENT_VALUE );
        assertEquals( "The event does not meet the equals contract for reflexivity.", event, event );

        // Symmetric
        Event<String> otherEvent = Event.of( Instant.parse( FIFTH_TIME ), EVENT_VALUE );
        assertTrue( "The event does not meet the equals contract for symmetry.",
                    event.equals( otherEvent ) && otherEvent.equals( event ) );

        // Transitive
        Event<String> oneMoreEvent = Event.of( Instant.parse( FIFTH_TIME ), EVENT_VALUE );
        assertTrue( "The event does not meet the equals contract for transitivity.",
                    event.equals( otherEvent ) && otherEvent.equals( oneMoreEvent ) && event.equals( oneMoreEvent ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( "The event does not meet the equals contract for consistency.", event, otherEvent );
        }

        // Nullity
        assertNotEquals( null, event );

        // Check unequal cases for time and value
        Event<String> unequalOnTime = Event.of( Instant.parse( "1985-01-06T12:00:00Z" ), EVENT_VALUE );
        assertNotEquals( "Expected the event to differ on time.", event, unequalOnTime );

        Event<String> unequalOnValue = Event.of( Instant.parse( FIFTH_TIME ), "otherValue" );
        assertNotEquals( "Expected the event to differ on value.", event, unequalOnValue );
    }

    @Test
    public void testHashCode()
    {
        // Consistent with equals
        Event<String> event = Event.of( Instant.parse( FIFTH_TIME ), EVENT_VALUE );
        Event<String> otherEvent = Event.of( Instant.parse( FIFTH_TIME ), EVENT_VALUE );
        assertTrue( "The hashcode of the event is inconsistent with equals.",
                    event.equals( otherEvent ) && event.hashCode() == otherEvent.hashCode() );
        // Consistent when called repeatedly
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( "The event does not meet the hashcode contract for consistency.",
                          event.hashCode(),
                          otherEvent.hashCode() );
        }
    }

    @Test
    public void testToString()
    {
        Event<String> event = Event.of( Instant.parse( FIFTH_TIME ), EVENT_VALUE );
        assertEquals( "(1985-01-05T12:00:00Z,someValue)", event.toString() );
    }

    @Test
    public void testCompareTo()
    {
        Event<String> event = Event.of( Instant.parse( FIRST_TIME ), EVENT_VALUE );
        Event<String> isEqual = Event.of( Instant.parse( FIRST_TIME ), EVENT_VALUE );
        Event<String> isLess = Event.of( Instant.parse( SECOND_TIME ), EVENT_VALUE );
        Event<String> isGreater = Event.of( Instant.parse( THIRD_TIME ), EVENT_VALUE );

        assertEquals( 0, event.compareTo( isEqual ) );
        assertTrue( event.compareTo( isLess ) < 0 );
        assertTrue( event.compareTo( isGreater ) > 0 );

        Event<String> eventTwo = Event.of( Instant.parse( FIRST_TIME ), EVENT_VALUE );
        Event<String> isEqualTwo = Event.of( Instant.parse( FIRST_TIME ), EVENT_VALUE );
        Event<String> isLessTwo = Event.of( Instant.parse( SECOND_TIME ), EVENT_VALUE );
        Event<String> isGreaterTwo = Event.of( Instant.parse( THIRD_TIME ), EVENT_VALUE );

        assertEquals( 0, eventTwo.compareTo( isEqualTwo ) );
        assertTrue( eventTwo.compareTo( isLessTwo ) < 0 );
        assertTrue( eventTwo.compareTo( isGreaterTwo ) > 0 );

        // Consistent with equals
        Event<String> eventOneEqual = Event.of( Instant.parse( FIRST_TIME ), EVENT_VALUE );
        Event<String> eventTwoEqual = Event.of( Instant.parse( FIRST_TIME ), EVENT_VALUE );
        Event<String> eventOneUnequalOnValue = Event.of( Instant.parse( FIRST_TIME ), "differentValue" );

        assertEquals( 0, eventOneEqual.compareTo( eventTwoEqual ) );
        assertNotEquals( 0, eventOneEqual.compareTo( eventOneUnequalOnValue ) );
    }

    /**
     * Constructs an {@link Event} and tests for an exception when the event time is null.
     */

    @Test
    public void testExceptionOnNullEventTime()
    {
        assertThrows( NullPointerException.class, () -> Event.of( null, EVENT_VALUE ) );
    }

    @Test
    public void testExceptionOnNullEvent()
    {
        Instant time = Instant.parse( FOURTH_TIME );
        assertThrows( NullPointerException.class, () -> Event.of( time, null ) );
    }

    @Test
    public void testExceptionOnNullInputForComparison()
    {
        Event<String> event = Event.of( Instant.parse( FOURTH_TIME ), "someEvent" );
        assertThrows( NullPointerException.class,
                      () -> event.compareTo( null ) );
    }

}
