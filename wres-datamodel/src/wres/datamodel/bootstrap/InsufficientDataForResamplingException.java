package wres.datamodel.bootstrap;

/**
 * An exception encountered when attempting to conduct resampling with insufficient data.
 * @author James Brown
 */

public class InsufficientDataForResamplingException extends RuntimeException
{
    /**
     * @param message the message
     */
    public InsufficientDataForResamplingException( String message )
    {
        super( message );
    }
}
