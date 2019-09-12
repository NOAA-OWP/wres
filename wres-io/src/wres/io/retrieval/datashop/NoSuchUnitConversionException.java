package wres.io.retrieval.datashop;

/**
 * Runtime exception associated with a unit conversion.
 * 
 * @author james.brown@hydrosolved.com
 */

public class NoSuchUnitConversionException extends RuntimeException
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = -4437476823446392472L;

    /**
     * Constructs an {@link NoSuchUnitConversionException} with no message.
     */

    public NoSuchUnitConversionException()
    {
        super();
    }

    /**
     * Constructs a {@link NoSuchUnitConversionException} with the specified message.
     * 
     * @param message the message.
     */

    public NoSuchUnitConversionException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link NoSuchUnitConversionException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public NoSuchUnitConversionException(final String message, final Throwable cause)
    {
        super(message, cause);
    }    
    
    
}
