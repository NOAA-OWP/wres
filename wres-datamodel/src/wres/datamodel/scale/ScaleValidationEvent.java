package wres.datamodel.scale;

import java.util.Objects;

/**
 * Records a validation event related to scale information or rescaling. There are two types of validation event, 
 * represented by the {@link EventType} enumeration within this class, namely {@link EventType#WARN}, which represents 
 * a warning and {@link EventType#ERROR}, which represents an error. A {@link ScaleValidationEvent} provides a 
 * mechanism to collect together multiple validation events before acting or reporting on them collectively, such as by 
 * throwing an exception with a summary of all validation events encountered, rather than drip-feeding validation 
 * failures.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ScaleValidationEvent
{
    /**
     * Describes a type of scale validation event.
     * 
     * @author james.brown@hydrosolved.com
     */

    public enum EventType
    {
        /**
         * An event that represents a warning.
         */
        WARN,

        /**
         * An event that represents an error.
         */
        ERROR;
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
