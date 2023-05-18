package wres.io.reading.wrds.thresholds;

import java.io.Serial;

/**
 * Runtime exception associated with threshold reading.
 * 
 * @author James Brown
 */

class ThresholdReadingException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    @Serial
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
