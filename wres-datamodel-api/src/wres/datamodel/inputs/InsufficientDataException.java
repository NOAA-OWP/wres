package wres.datamodel.inputs;

/**
 * Runtime exception that denotes insufficient data for metric computation.
 * 
 * @author jesse
 * @version 0.1
 * @since 0.3
 */

public class InsufficientDataException extends MetricInputException
{
    
    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = 3624806551414207142L;

    /**
     * Constructs a {@link MetricInputException} with the specified message.
     * 
     * @param message the message.
     */
    
    public InsufficientDataException( String message)
    {
        super( message );
    }
    
    /**
     * Constructs a {@link InsufficientDataException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public InsufficientDataException(final String message, final Throwable cause)
    {
        super(message, cause);
    }
    
    
}
