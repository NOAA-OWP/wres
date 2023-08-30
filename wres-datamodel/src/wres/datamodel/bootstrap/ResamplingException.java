package wres.datamodel.bootstrap;

/**
 * An exception encountered when resampling time-series.
 * @author James Brown
 */

class ResamplingException extends RuntimeException
{
    /**
     * @param message the message
     */
    public ResamplingException( String message )
    {
        super( message );
    }

    /**
     * @param message the message
     * @param cause the cause
     */
    public ResamplingException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
