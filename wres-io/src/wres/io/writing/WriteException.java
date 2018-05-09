package wres.io.writing;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with writing metric outputs.
 * 
 * @author james.brown@hydrosolved.com
 */

public class WriteException extends DataException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = -8379791452118960970L;

    /**
     * Constructs an {@link WriteException} with no message.
     */

    public WriteException()
    {
        super();
    }

    /**
     * Constructs a {@link WriteException} with the specified message.
     * 
     * @param message the message.
     */

    public WriteException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link WriteException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public WriteException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

}
