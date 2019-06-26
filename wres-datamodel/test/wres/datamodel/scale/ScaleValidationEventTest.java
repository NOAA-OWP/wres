package wres.datamodel.scale;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.scale.ScaleValidationEvent.EventType;

/**
 * Tests the {@link ScaleValidationEvent}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ScaleValidationEventTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private final ScaleValidationEvent event = ScaleValidationEvent.of( EventType.WARN, "A warning" );

    @Test
    public void testConstructionOfWarnEvent()
    {
        assertThat( ScaleValidationEvent.warn( "A warning" ), equalTo( this.event ) );
    }

    @Test
    public void testConstructionOfErrorEvent()
    {
        assertThat( ScaleValidationEvent.error( "An error" ),
                    equalTo( ScaleValidationEvent.of( EventType.ERROR, "An error" ) ) );
    }

    @Test
    public void testConstructionOfPassEvent()
    {
        assertThat( ScaleValidationEvent.pass( "No issues" ),
                    equalTo( ScaleValidationEvent.of( EventType.PASS, "No issues" ) ) );
    }
    
    @Test
    public void testGetEventTypeReturnsExpectedType()
    {
        assertThat( EventType.WARN, equalTo( this.event.getEventType() ) );
    }

    @Test
    public void testGetMessageReturnsExpectedString()
    {
        assertThat( "A warning", equalTo( this.event.getMessage() ) );
    }

    @Test
    public void testToStringReturnsExpectedString()
    {
        assertThat( "WARN: A warning", equalTo( this.event.toString() ) );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertThat( this.event, equalTo( this.event ) );

        // Symmetric
        ScaleValidationEvent otherEvent = ScaleValidationEvent.of( EventType.WARN, "A warning" );
        assertThat( this.event, equalTo( otherEvent ) );

        // Transitive, noting that event and otherEvent are equal above
        ScaleValidationEvent oneMoreEvent = ScaleValidationEvent.of( EventType.WARN, "A warning" );
        assertThat( otherEvent, equalTo( oneMoreEvent ) );
        assertThat( this.event, equalTo( oneMoreEvent ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertThat( this.event, equalTo( this.event ) );
        }

        // Object unequal to null
        assertThat( this.event, not( equalTo( null ) ) );

        // Unequal on event type
        ScaleValidationEvent anError = ScaleValidationEvent.of( EventType.PASS, "A warning" );
        assertThat( this.event, not( equalTo( anError ) ) );

        // Unequal on message
        ScaleValidationEvent aWarning = ScaleValidationEvent.of( EventType.WARN, "An error" );
        assertThat( this.event, not( equalTo( aWarning ) ) );

    }

    @Test
    public void testHashcode()
    {
        // Consistent with equals 
        ScaleValidationEvent otherEvent = ScaleValidationEvent.of( EventType.WARN, "A warning" );

        assertThat( this.event.hashCode(), equalTo( otherEvent.hashCode() ) );

        ScaleValidationEvent anError = ScaleValidationEvent.of( EventType.ERROR, "An error" );
        ScaleValidationEvent anotherError = ScaleValidationEvent.of( EventType.ERROR, "An error" );

        assertThat( anError.hashCode(), equalTo( anotherError.hashCode() ) );

        // Repeatable within one execution context
        for ( int i = 0; i < 100; i++ )
        {
            assertThat( this.event.hashCode(), equalTo( otherEvent.hashCode() ) );
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
        
        exception.expect( NullPointerException.class );

        this.event.compareTo( null );
    }

    @Test
    public void testThrowNPEOnConstructionIfEventTypeIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify a non-null event type for the scale validation event." );

        ScaleValidationEvent.of( null, "A message" );
    }

    @Test
    public void testThrowNPEOnConstructionIfMessageIsNull()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify a non-null message for the scale validation event." );

        ScaleValidationEvent.of( EventType.ERROR, null );
    }

}
