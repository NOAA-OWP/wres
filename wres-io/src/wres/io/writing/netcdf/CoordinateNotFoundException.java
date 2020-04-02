package wres.io.writing.netcdf;

public class CoordinateNotFoundException extends Exception
{

    private static final long serialVersionUID = 194816125215945933L;

    public CoordinateNotFoundException( final String message )
    {
        super( message );
    }

    public CoordinateNotFoundException( final String message, Throwable cause )
    {
        super( message, cause );
    }


}
