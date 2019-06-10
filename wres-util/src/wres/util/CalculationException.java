package wres.util;

/**
 * Indicates an exceptional event that was caused by some sort of calculation error,
 * such as a function being unable to interact with factors needed to perform
 * psuedo-mathematical computations
 */
public class CalculationException extends RuntimeException
{
    public CalculationException( String message, Throwable cause)
    {
        super(message, cause);
    }
    public CalculationException(String message) {
        super(message);
    }
}
