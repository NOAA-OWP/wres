package wres.config.yaml;

/**
 * An exception encountered on validating a project declaration string against a project declaration schema.
 * 
 * @author James Brown
 */

class SchemaValidationException extends RuntimeException
{
    /**
     * Constructs a {@link SchemaValidationException} with the specified message.
     * 
     * @param message the message.
     */

    SchemaValidationException( final String message )
    {
        super( message );
    }

}