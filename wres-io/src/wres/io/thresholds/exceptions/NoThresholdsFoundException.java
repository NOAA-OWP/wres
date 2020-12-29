package wres.io.thresholds.exceptions;

/**
 * Runtime exception indicating no thresholds were found, possibly after filtering by user declaration.
 * 
 * @author james.brown@hydrosolved.com
 */

public class NoThresholdsFoundException extends RuntimeException
{

    private static final long serialVersionUID = 7814760186227946523L;

    /**
     * Build with an error message.
     * @param message the error message
     */
    
    public NoThresholdsFoundException( String message )
    {
        super( message );
    }
    
}
