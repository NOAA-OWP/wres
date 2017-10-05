package wres.io.reading;

/**
 * An issue was found with the structure or content of source data.
 *
 * <p>
 *
 * Use this when a required field/variable/tag is not found or when data is
 * found to be unparseable.
 *
 * </p>
 */
public class InvalidInputDataException extends IngestException
{
    public InvalidInputDataException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public InvalidInputDataException( String message )
    {
        super( message );
    }
}
