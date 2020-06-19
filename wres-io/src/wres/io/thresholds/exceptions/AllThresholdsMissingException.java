package wres.io.thresholds.exceptions;

/**
 * Used to identify an exception that originates from all thresholds matching the missing
 * value.
 */

public class AllThresholdsMissingException extends IllegalArgumentException
{
    private static final long serialVersionUID = -7265565803593007243L;

    public AllThresholdsMissingException( String message )
    {
        super( message );
    }
}