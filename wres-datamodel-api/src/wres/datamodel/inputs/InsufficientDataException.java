package wres.datamodel.inputs;

public class InsufficientDataException extends MetricInputException
{
    public InsufficientDataException( String message)
    {
        super( message );
    }
}
