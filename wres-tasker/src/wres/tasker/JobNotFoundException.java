package wres.tasker;

public class JobNotFoundException extends IllegalStateException
{
    JobNotFoundException( String message )
    {
        super( message );
    }
}

