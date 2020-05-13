package wres.statistics;

/**
 * Exception to throw when a statistics message could not be posted.
 * 
 * @author james.brown@hydrosolved.com
 */

public class StatisticsMessageException extends RuntimeException
{
    private static final long serialVersionUID = -7716500066344674814L;

    /**
     * Builds an exception with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public StatisticsMessageException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
