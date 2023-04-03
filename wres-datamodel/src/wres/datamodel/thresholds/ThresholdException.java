package wres.datamodel.thresholds;

import java.io.Serial;

/**
 * An unchecked exception related to thresholds.
 * 
 * @author James Brown
 */

public final class ThresholdException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = 1005982342026773021L;

    /**
     * Constructs a {@link ThresholdException} with the specified message.
     * 
     * @param message the message.
     */

    public ThresholdException(final String message)
    {
        super(message);
    }
}
