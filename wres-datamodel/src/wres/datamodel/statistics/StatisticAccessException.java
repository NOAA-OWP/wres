package wres.datamodel.statistics;

/**
 * A checked exception that is thrown on attempting to access a {@link Statistic} that is not available.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class StatisticAccessException extends Exception
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 96812230759775484L;

    /**
     * Constructs an {@link StatisticAccessException} with no message.
     */

    public StatisticAccessException()
    {
        super();
    }

    /**
     * Constructs a {@link StatisticAccessException} with the specified message.
     * 
     * @param message the message.
     */

    public StatisticAccessException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link StatisticAccessException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public StatisticAccessException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
