package wres.datamodel.statistics;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with an incorrect {@link Statistic}.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class StatisticException extends DataException
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = -1559780940246528464L;
    
    /**
     * Constructs an {@link StatisticException} with no message.
     */

    public StatisticException()
    {
        super();
    }

    /**
     * Constructs a {@link StatisticException} with the specified message.
     * 
     * @param message the message.
     */

    public StatisticException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link StatisticException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public StatisticException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
