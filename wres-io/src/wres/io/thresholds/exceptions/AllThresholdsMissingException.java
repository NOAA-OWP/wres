package wres.io.thresholds.exceptions;

import java.io.Serial;

/**
 * Used to identify an exception that originates from all thresholds matching the missing
 * value.
 */

public class AllThresholdsMissingException extends IllegalArgumentException
{
    @Serial
    private static final long serialVersionUID = -7265565803593007243L;

    /**
     * Creates an instance.
     * @param message the message
     */
    public AllThresholdsMissingException( String message )
    {
        super( message );
    }
}