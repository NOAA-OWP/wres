package wres.datamodel.messages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.EvaluationStage;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;

/**
 * Tests the {@link EvaluationStatusMessage}.
 * 
 * @author James Brown
 */
public final class EvaluationStatusMessageTest
{

    private final EvaluationStatusMessage event = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                              EvaluationStage.POOLING,
                                                                              "A warning" );

    @Test
    public void testConstructionOfWarnEvent()
    {
        assertEquals( EvaluationStatusMessage.warn( EvaluationStage.POOLING, "A warning" ), this.event );
    }

    @Test
    public void testConstructionOfErrorEvent()
    {
        assertEquals( EvaluationStatusMessage.error( EvaluationStage.POOLING, "An error" ),
                      EvaluationStatusMessage.of( StatusLevel.ERROR, EvaluationStage.POOLING, "An error" ) );
    }

    @Test
    public void testConstructionOfDebugEvent()
    {
        assertEquals( EvaluationStatusMessage.debug( EvaluationStage.POOLING, "No issues" ),
                      EvaluationStatusMessage.of( StatusLevel.DEBUG, EvaluationStage.POOLING, "No issues" ) );
    }

    @Test
    public void testGetEventTypeReturnsExpectedType()
    {
        assertEquals( StatusLevel.WARN, this.event.getStatusLevel() );
    }

    @Test
    public void testGetMessageReturnsExpectedString()
    {
        assertEquals( "A warning", this.event.getMessage() );
    }

    @Test
    public void testToStringReturnsExpectedString()
    {
        assertEquals( "EvaluationStatusMessage[LEVEL=WARN,STAGE=POOLING,MESSAGE=A warning]", this.event.toString() );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertEquals( this.event, this.event );

        // Symmetric
        EvaluationStatusMessage otherEvent = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                         EvaluationStage.POOLING,
                                                                         "A warning" );
        assertEquals( this.event, otherEvent );

        // Transitive, noting that event and otherEvent are equal above
        EvaluationStatusMessage oneMoreEvent = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                           EvaluationStage.POOLING,
                                                                           "A warning" );
        assertEquals( otherEvent, oneMoreEvent );
        assertEquals( this.event, oneMoreEvent );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.event, this.event );
        }

        // Object unequal to null
        assertNotEquals( this.event, null );

        // Unequal on status level
        EvaluationStatusMessage aDebug = EvaluationStatusMessage.of( StatusLevel.DEBUG,
                                                                     EvaluationStage.POOLING,
                                                                     "A warning" );
        assertNotEquals( this.event, aDebug );

        // Unequal on evaluation stage
        EvaluationStatusMessage aWarnIngest = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                          EvaluationStage.INGESTING,
                                                                          "A warning" );
        assertNotEquals( this.event, aWarnIngest );

        // Unequal on message
        EvaluationStatusMessage aWarning = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                       EvaluationStage.POOLING,
                                                                       "An error" );
        assertNotEquals( this.event, aWarning );
    }

    @Test
    public void testHashcode()
    {
        // Consistent with equals 
        EvaluationStatusMessage otherEvent = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                         EvaluationStage.POOLING,
                                                                         "A warning" );

        assertEquals( this.event.hashCode(), otherEvent.hashCode() );

        EvaluationStatusMessage anError = EvaluationStatusMessage.of( StatusLevel.ERROR,
                                                                      EvaluationStage.POOLING,
                                                                      "An error" );
        EvaluationStatusMessage anotherError = EvaluationStatusMessage.of( StatusLevel.ERROR,
                                                                           EvaluationStage.POOLING,
                                                                           "An error" );

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
        EvaluationStatusMessage otherEvent = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                         EvaluationStage.POOLING,
                                                                         "A warning" );

        assertEquals( 0, this.event.compareTo( otherEvent ) );

        EvaluationStatusMessage anError = EvaluationStatusMessage.of( StatusLevel.ERROR,
                                                                      EvaluationStage.POOLING,
                                                                      "1" );
        EvaluationStatusMessage anotherError = EvaluationStatusMessage.of( StatusLevel.ERROR,
                                                                           EvaluationStage.POOLING,
                                                                           "2" );

        assertTrue( anError.compareTo( anotherError ) < 0 );

        assertTrue( anotherError.compareTo( anError ) > 0 );

        EvaluationStatusMessage otherEventError = EvaluationStatusMessage.of( StatusLevel.ERROR,
                                                                              EvaluationStage.POOLING,
                                                                              "A warning" );

        assertTrue( otherEvent.compareTo( otherEventError ) > 0 );

        EvaluationStatusMessage otherEventWarn = EvaluationStatusMessage.of( StatusLevel.WARN,
                                                                             EvaluationStage.INGESTING,
                                                                             "A warning" );

        assertTrue( otherEvent.compareTo( otherEventWarn ) > 0 );

        assertThrows( NullPointerException.class, () -> this.event.compareTo( null ) );
    }

    @Test
    public void testThrowNPEOnConstructionIfEventTypeIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> EvaluationStatusMessage.of( null,
                                                        EvaluationStage.POOLING,
                                                        "A message" ) );
    }

    @Test
    public void testThrowNPEOnConstructionIfEvaluationStatusIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> EvaluationStatusMessage.of( StatusLevel.ERROR,
                                                        null,
                                                        "A message" ) );
    }

    @Test
    public void testThrowNPEOnConstructionIfMessageIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> EvaluationStatusMessage.of( StatusLevel.ERROR,
                                                        EvaluationStage.POOLING,
                                                        null ) );
    }

}
