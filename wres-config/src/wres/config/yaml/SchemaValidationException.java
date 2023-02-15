package wres.config.yaml;

/**
 * An exception encountered on validating a project declaration string against a project declaration schema.
 * 
 * @author James Brown
 */

class SchemaValidationException extends RuntimeException
{
    /** Serial ID. */
    private static final long serialVersionUID = -8131650631668348496L;

    /**
     * Constructs a {@link DataException} with the specified message.
     * 
     * @param message the message.
     */

    SchemaValidationException( final String message )
    {
        super( message );
    }

}