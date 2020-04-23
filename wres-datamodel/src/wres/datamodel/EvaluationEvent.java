package wres.datamodel;

/**
 * <p>An API for propagating externally facing information to callers that should not be buried in logging. For 
 * example, use this API to warn users about assumptions made during an evaluation.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface EvaluationEvent
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
         * An event that represents a debug level of information, but still externally facing, i.e. not intended for 
         * developers.
         */
        DEBUG,
        
        /**
         * An event that represents an error.
         */
        ERROR,
        
        /**
         * A neutral information message.
         */
        INFO;
    }

    /**
     * Returns the event type.
     * 
     * @return the event type
     */

    public EventType getEventType();

    /**
     * Returns the message string.
     * 
     * @return the message
     */

    public String getMessage();

}
