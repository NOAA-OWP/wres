package wres.config.yaml;

/**
 * An exception encountered on validating a project declaration.
 * 
 * @author James Brown
 */

class DeclarationValidationException extends RuntimeException
{
    /**
     * Constructs a {@link DeclarationValidationException} with the specified message.
     *
     * @param message the message.
     */

    DeclarationValidationException( final String message )
    {
        super( message );
    }

}