package wres.datamodel.scale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.scale.ScaleValidationEvent.EventType;

/**
 * Tests the {@link ScaleValidationEvent}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ScaleValidationEventTest
{

    private final ScaleValidationEvent event = ScaleValidationEvent.of( EventType.WARN, "A warning" );

    @Test
    public void testConstructionOfWarnEvent()
    {
        assertEquals( ScaleValidationEvent.warn( "A warning" ), this.event );
    }

    @Test
    public void testConstructionOfErrorEvent()
    {
        assertEquals( ScaleValidationEvent.error( "An error" ),
                      ScaleValidationEvent.of( EventType.ERROR, "An error" ) );
    }

    @Test
    public void testConstructionOfPassEvent()
    {
        assertEquals( ScaleValidationEvent.debug( "No issues" ),
                      ScaleValidationEvent.of( EventType.DEBUG, "No issues" ) );
    }

    @Test
    public void testGetEventTypeReturnsExpectedType()
    {
        assertEquals( EventType.WARN, this.event.getEventType() );
    }

    @Test
    public void testGetMessageReturnsExpectedString()
    {
        assertEquals( "A warning", this.event.getMessage() );
    }

    @Test
    public void testToStringReturnsExpectedString()
    {
        assertEquals( "WARN: A warning", this.event.toString() );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertEquals( this.event, this.event );

        // Symmetric
        ScaleValidationEvent otherEvent = ScaleValidationEvent.of( EventType.WARN, "A warning" );
        assertEquals( this.event, otherEvent );

        // Transitive, noting that event and otherEvent are equal above
        ScaleValidationEvent oneMoreEvent = ScaleValidationEvent.of( EventType.WARN, "A warning" );
        assertEquals( otherEvent, oneMoreEvent );
        assertEquals( this.event, oneMoreEvent );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.event, this.event );
        }

        // Object unequal to null
        assertNotEquals( this.event, null );

        // Unequal on event type
        ScaleValidationEvent aDebug = ScaleValidationEvent.of( EventType.DEBUG, "A warning" );
        assertNotEquals( this.event, aDebug );

        // Unequal on message
        ScaleValidationEvent aWarning = ScaleValidationEvent.of( EventType.WARN, "An error" );
        assertNotEquals( this.event, aWarning );
    }

    @Test
    public void testHashcode()
    {
        // Consistent with equals 
        ScaleValidationEvent otherEvent = ScaleValidationEvent.of( EventType.WARN, "A warning" );

        assertEquals( this.event.hashCode(), otherEvent.hashCode() );

        ScaleValidationEvent anError = ScaleValidationEvent.of( EventType.ERROR, "An error" );
        ScaleValidationEvent anotherError = ScaleValidationEvent.of( EventType.ERROR, "An error" );

        assertEquals( anError.hashCode(), anotherError.hashCode() );

        // Repeatable within one execution context
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.event.hashCode(), otherEvent.hashCode() );
        }

    }

    @Test
    public void testCompareTo()
    {
        // Consistent with equals 
        ScaleValidationEvent otherEvent = ScaleValidationEvent.of( EventType.WARN, "A warning" );

        assertTrue( this.event.compareTo( otherEvent ) == 0 );

        ScaleValidationEvent anError = ScaleValidationEvent.of( EventType.ERROR, "1" );
        ScaleValidationEvent anotherError = ScaleValidationEvent.of( EventType.ERROR, "2" );

        assertTrue( anError.compareTo( anotherError ) < 0 );

        assertTrue( anotherError.compareTo( anError ) > 0 );

        ScaleValidationEvent otherEventError = ScaleValidationEvent.of( EventType.ERROR, "A warning" );

        assertTrue( otherEvent.compareTo( otherEventError ) < 0 );

        assertThrows( NullPointerException.class, () -> this.event.compareTo( null ) );
    }

    @Test
    public void testThrowNPEOnConstructionIfEventTypeIsNull()
    {
        NullPointerException exception =
                assertThrows( NullPointerException.class, () -> ScaleValidationEvent.of( null, "A message" ) );

        assertEquals( "Specify a non-null event type for the scale validation event.", exception.getMessage() );
    }

    @Test
    public void testThrowNPEOnConstructionIfMessageIsNull()
    {
        NullPointerException exception =
                assertThrows( NullPointerException.class, () -> ScaleValidationEvent.of( EventType.ERROR, null ) );

        assertEquals( "Specify a non-null message for the scale validation event.", exception.getMessage() );
    }

}
