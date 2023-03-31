package wres.config.yaml;

/**
 * An exception encountered on validating a project declaration.
 * 
 * @author James Brown
 */

class DeclarationException extends RuntimeException
{
    /**
     * Constructs a {@link DeclarationException} with the specified message.
     *
     * @param message the message.
     */

    DeclarationException( final String message )
    {
        super( message );
    }
}