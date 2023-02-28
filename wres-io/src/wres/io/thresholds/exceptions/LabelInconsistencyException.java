package wres.io.thresholds.exceptions;

import java.io.Serial;

/**
 * Used to identify an exception that originates from an inconsistency between the number
 * of labels and the number of thresholds.
 */
public class LabelInconsistencyException extends IllegalArgumentException
{
    @Serial
    private static final long serialVersionUID = 4507239538788881616L;

    /**
     * Creates an instance.
     * @param message the message
     */
    public LabelInconsistencyException( String message )
    {
        super( message );
    }
}