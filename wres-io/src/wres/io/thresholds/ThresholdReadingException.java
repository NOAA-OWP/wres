package wres.io.thresholds;

/**
 * Runtime exception associated with threshold reading.
 * 
 * @author james.brown@hydrosolved.com
 */

class ThresholdReadingException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = -6618305285198312862L;

    /**
     * Constructs a {@link ThresholdReadingException} with the specified message.
     * 
     * @param message the message.
     */

    public ThresholdReadingException(final String message)
    {
        super(message );
    }  
    
    /**
     * Constructs a {@link ThresholdReadingException} with the specified message and cause.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public ThresholdReadingException(final String message, final Throwable cause)
    {
        super(message, cause);
    }    
    
    
}
