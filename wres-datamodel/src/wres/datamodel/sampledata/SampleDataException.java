package wres.datamodel.sampledata;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with incorrect {@link SampleData}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SampleDataException extends DataException
{

    /**
     * Serial identifier.
     */
    private static final long serialVersionUID = -382138122319205095L;

    /**
     * Constructs an {@link SampleDataException} with no message.
     */

    public SampleDataException()
    {
        super();
    }

    /**
     * Constructs a {@link SampleDataException} with the specified message.
     * 
     * @param message the message.
     */

    public SampleDataException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link SampleDataException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public SampleDataException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
