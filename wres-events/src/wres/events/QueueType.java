package wres.events;

/**
 * Enumeration of queue types on the broker.
 *
 * @author James Brown
 */

public enum QueueType
{
    /** Name for the queue on the amq.topic that accepts evaluation status messages. */
    EVALUATION_STATUS_QUEUE,

    /** Name for the queue on the amq.topic that accepts evaluation status messages. */
    EVALUATION_QUEUE,

    /** Name for the queue on the amq.topic that accepts evaluation status messages. */
    STATISTICS_QUEUE,
    
    /** Name for the queue on the amq.topic that accepts pairs messages. */
    PAIRS_QUEUE;

    /**
     * @return a string representation.
     */
    @Override
    public String toString()
    {
        return switch ( this )
                {
                    case EVALUATION_STATUS_QUEUE -> "status";
                    case EVALUATION_QUEUE -> "evaluation";
                    case STATISTICS_QUEUE -> "statistics";
                    case PAIRS_QUEUE -> "pairs";
                };
    }
}

