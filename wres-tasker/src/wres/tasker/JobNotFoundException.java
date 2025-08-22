package wres.tasker;

/**
 * Exception when a job cannot be found.
 */

public class JobNotFoundException extends IllegalStateException
{
    /**
     * Creates an instance.
     * @param message the message
     */
    JobNotFoundException( String message )
    {
        super( message );
    }
}

