package wres.io.writing.netcdf;

import wres.io.writing.WriteException;

/**
 * A runtime exception encountered when writing statistics to NetCDF format.
 * 
 * @author James Brown
 */

public class NetcdfWriteException extends WriteException
{
    private static final long serialVersionUID = 4078132138204917922L;

    /**
     * @param message the message
     */
    public NetcdfWriteException( final String message )
    {
        super( message );
    }

    /**
     * @param message the message
     * @param cause the cause
     */
    public NetcdfWriteException( final String message, Throwable cause )
    {
        super( message, cause );
    }

}
