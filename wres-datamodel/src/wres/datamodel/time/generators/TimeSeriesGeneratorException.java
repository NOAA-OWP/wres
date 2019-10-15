package wres.datamodel.time.generators;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with the functional generation of time-series.
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesGeneratorException extends DataException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = -8585419352611659835L;

    /**
     * Constructs an {@link TimeSeriesGeneratorException} with no message.
     */

    public TimeSeriesGeneratorException()
    {
        super();
    }

    /**
     * Constructs a {@link TimeSeriesGeneratorException} with the specified message.
     * 
     * @param message the message.
     */

    public TimeSeriesGeneratorException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link TimeSeriesGeneratorException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public TimeSeriesGeneratorException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
