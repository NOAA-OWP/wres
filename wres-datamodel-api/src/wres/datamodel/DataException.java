package wres.datamodel;

/**
 * A base class for a runtime exception associated with a dataset.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DataException extends RuntimeException
{

    /**
     * Serial ID.
     */
    
    private static final long serialVersionUID = -9205464442064407973L;

    /**
     * Constructs an {@link DataException} with no message.
     */

    public DataException()
    {
        super();
    }

    /**
     * Constructs a {@link DataException} with the specified message.
     * 
     * @param message the message.
     */

    public DataException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link DataException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public DataException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
