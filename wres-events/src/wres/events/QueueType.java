package wres.events;

/**
 * Enumeration of queue types on the broker.
 *
 * @author james.brown@hydrosolved.com
 */

public enum QueueType
{
    /**
     * Name for the queue on the amq.topic that accepts evaluation status messages.
     */

    EVALUATION_STATUS_QUEUE,

    /**
     * Name for the queue on the amq.topic that accepts evaluation status messages.
     */

    EVALUATION_QUEUE,

    /**
     * Name for the queue on the amq.topic that accepts evaluation status messages.
     */

    STATISTICS_QUEUE,
    
    /**
     * Name for the queue on the amq.topic that accepts pairs messages.
     */    
    
    PAIRS_QUEUE;

    /**
     * @return a string representation.
     */
    @Override
    public String toString()
    {
        switch ( this )
        {
            case EVALUATION_STATUS_QUEUE:
                return "status";
            case EVALUATION_QUEUE:
                return "evaluation";
            case STATISTICS_QUEUE:
                return "statistics";
            case PAIRS_QUEUE:
                return "pairs";
            default:
                throw new IllegalArgumentException( "Unknown queue '" + this + "'." );
        }
    }
}

