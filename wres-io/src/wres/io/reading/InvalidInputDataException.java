package wres.io.reading;

import wres.io.ingesting.IngestException;

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
    private static final long serialVersionUID = -2539358043781861403L;

    public InvalidInputDataException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public InvalidInputDataException( String message )
    {
        super( message );
    }
}
