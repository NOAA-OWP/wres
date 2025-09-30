package wres.config;

/**
 * An exception encountered on reading, migrating or validating a project declaration.
 *
 * @author James Brown
 */

public class DeclarationException extends RuntimeException
{
    /**
     * Creates an instance.
     *
     * @param message the message.
     */

    public DeclarationException( String message )
    {
        super( message );
    }

    /**
     * Creates an instance with the specified message and cause.
     * @param message the message
     * @param cause the cause
     */
    public DeclarationException( String message, Throwable cause )
    {
        super( message, cause );
    }
}