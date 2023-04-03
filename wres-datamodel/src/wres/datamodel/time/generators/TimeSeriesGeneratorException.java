package wres.datamodel.time.generators;

import java.io.Serial;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with the functional generation of time-series.
 * 
 * @author James Brown
 */

public class TimeSeriesGeneratorException extends DataException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = -8585419352611659835L;

    /**
     * Constructs a {@link TimeSeriesGeneratorException} with the specified message.
     * 
     * @param message the message.
     */

    public TimeSeriesGeneratorException(final String message)
    {
        super(message);
    }
}
