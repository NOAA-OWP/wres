package wres.datamodel.units;

import java.io.Serial;

/**
 * Runtime exception associated with a unit conversion.
 * 
 * @author James Brown
 */

public class NoSuchUnitConversionException extends RuntimeException
{
    /**
     * Serial identifier.
     */
    
    @Serial
    private static final long serialVersionUID = -4437476823446392472L;

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
