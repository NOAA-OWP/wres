package wres.datamodel.scale;

import java.util.Objects;

import wres.statistics.generated.EvaluationStatus;

/**
 * <p>Records a validation event related to scale information or rescaling. A {@link ScaleValidationEvent} provides a 
 * mechanism to collect together multiple validation events before acting or reporting on them collectively, such as by 
 * throwing an exception with a summary of all validation events encountered, rather than drip-feeding validation 
 * failures.
 * 
 * <p>See #64542 for the near-term motivation and #61930 for the long-term motivation. While this abstraction may light
 * a path towards a more general messaging API for exceptions, warnings, and assumptions that are intended for users 
 * (rather than developers), it is more likely that a messaging API will replace this abstraction.
 * 
 * TODO: update 20200814. This class should wrap an {@link EvaluationStatus} message. The reason to wrap rather than 
 * replace is its implementation of {@link Comparable}.
 * 
 * @author James Brown
 */

public class ScaleValidationEvent implements Comparable<ScaleValidationEvent>
{
    /**
     * Describes a type of scale validation event.
     * 
     * @author James Brown
     */

    public enum EventType
    {
        /** An event that represents a warning. */
        WARN,
        
        /** This should be interpreted as a detailed warning. It is externally facing, i.e. not intended for developers, 
         * but should be used when the information is detailed/specific or there are many such occurrences. */
        DEBUG,
        
        /** An event that represents an error. */
        ERROR,
        
        /** A neutral information message. */
        INFO;
    }
    
    /**
     * The event type.
     */

    private final EventType eventType;

    /**
     * The message.
     */

    private final String message;

    /**
     * Construct a validation event.
     * 
     * @param eventType the event type
     * @param message the message
     * @throws NullPointerException if either input is null
     * @return a validation event
     */

    public static ScaleValidationEvent of( EventType eventType, String message )
    {
        return new ScaleValidationEvent( eventType, message );
    }

    /**
     * Construct a validation event of type {@link EventType#WARN}.
     * 
     * @param message the message
     * @throws NullPointerException if the message is null
     * @return a validation event
     */

    public static ScaleValidationEvent warn( String message )
    {
        return new ScaleValidationEvent( EventType.WARN, message );
    }

    /**
     * Construct a validation event of type {@link EventType#ERROR}.
     * 
     * @param message the message
     * @throws NullPointerException if the message is null
     * @return a validation event
     */

    public static ScaleValidationEvent error( String message )
    {
        return new ScaleValidationEvent( EventType.ERROR, message );
    }

    /**
     * Construct a validation event of type {@link EventType#ERROR}.
     * 
     * @param message the message
     * @throws NullPointerException if the message is null
     * @return a validation event
     */

    public static ScaleValidationEvent info( String message )
    {
        return new ScaleValidationEvent( EventType.INFO, message );
    }

    /**
     * Construct a validation event of type {@link EventType#DEBUG}.
     * 
     * @param message the message
     * @throws NullPointerException if the message is null
     * @return a validation event
     */

    public static ScaleValidationEvent debug( String message )
    {
        return new ScaleValidationEvent( EventType.DEBUG, message );
    }

    /**
     * Returns the event type.
     * 
     * @return the event type
     */

    public EventType getEventType()
    {
        return this.eventType;
    }

    /**
     * Returns the message.
     * 
     * @return the validation message
     */

    public String getMessage()
    {
        return this.message;
    }

    /**
     * Provides a string representation of the validation event.
     * 
     * @return a string representation
     */

    @Override
    public String toString()
    {
        return eventType + ": " + message;
    }

    @Override
    public int compareTo( ScaleValidationEvent o )
    {
        Objects.requireNonNull( o );

        int returnMe = this.getEventType().compareTo( o.getEventType() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        return this.getMessage().compareTo( o.getMessage() );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( ! ( obj instanceof ScaleValidationEvent ) )
        {
            return false;
        }

        ScaleValidationEvent input = (ScaleValidationEvent) obj;

        return input.getEventType().equals( this.getEventType() ) && input.getMessage().equals( this.getMessage() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getEventType(), this.getMessage() );
    }

    /**
     * Hidden constructor.
     * 
     * @param eventType the event type
     * @param message the message
     * @throws NullPointerException if either input is null
     */

    private ScaleValidationEvent( EventType eventType, String message )
    {
        Objects.requireNonNull( eventType, "Specify a non-null event type for the scale validation event." );

        Objects.requireNonNull( message, "Specify a non-null message for the scale validation event." );

        this.eventType = eventType;

        this.message = message;
    }

}
