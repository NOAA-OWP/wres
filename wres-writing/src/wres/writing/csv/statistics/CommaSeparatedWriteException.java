package wres.writing.csv.statistics;

import java.io.Serial;

import wres.writing.WriteException;

/**
 * A runtime exception associated with writing metric outputs of Comma Separated Values (CSV).
 * 
 * @author James Brown
 */

class CommaSeparatedWriteException extends WriteException
{
    /** Serial identifier. */

    @Serial
    private static final long serialVersionUID = -9113524805560375493L;

    /**
     * Constructs a {@link CommaSeparatedWriteException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public CommaSeparatedWriteException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}
