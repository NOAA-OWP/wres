package wres.config;

/**
 * An exception encountered on validating a project declaration string against a project declaration schema.
 * 
 * @author James Brown
 */

class DeclarationSchemaException extends RuntimeException
{
    /**
     * Constructs a {@link DeclarationSchemaException} with the specified message.
     * 
     * @param message the message.
     */

    DeclarationSchemaException( final String message )
    {
        super( message );
    }
}