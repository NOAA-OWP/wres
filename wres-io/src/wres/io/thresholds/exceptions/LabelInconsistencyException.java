package wres.io.thresholds.exceptions;

/**
 * Used to identify an exception that originates from an inconsistency between the number
 * of labels and the number of thresholds.
 */

public class LabelInconsistencyException extends IllegalArgumentException
{
    private static final long serialVersionUID = 4507239538788881616L;

    public LabelInconsistencyException( String message )
    {
        super( message );
    }
}