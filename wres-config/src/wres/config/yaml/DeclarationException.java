package wres.config.yaml;

/**
 * An exception encountered on reading, migrating or validating a project declaration.
 *
 * @author James Brown
 */

class DeclarationException extends RuntimeException
{
    /**
     * Creates an instance.
     *
     * @param message the message.
     */

    DeclarationException( String message )
    {
        super( message );
    }

    /**
     * Creates an instance with the specified message and cause.
     * @param message the message
     * @param cause the cause
     */
    DeclarationException( String message, Throwable cause )
    {
        super( message, cause );
    }
}