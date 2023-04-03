package wres.datamodel.statistics;

import java.io.Serial;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with an incorrect {@link Statistic}.
 *
 * @author James Brown
 */

public final class StatisticException extends DataException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = -1559780940246528464L;

    /**
     * Constructs a {@link StatisticException} with the specified message.
     *
     * @param message the message.
     */

    public StatisticException( String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link StatisticException} with the specified message.
     *
     * @param message the message.
     * @param cause the cause of the exception
     */

    public StatisticException( String message, Throwable cause )
    {
        super( message, cause );
    }

}
