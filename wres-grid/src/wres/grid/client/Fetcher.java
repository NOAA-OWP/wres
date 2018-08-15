package wres.grid.client;

import java.io.IOException;

import ucar.ma2.InvalidRangeException;
import wres.io.griddedReader.GriddedReader;

/**
 * Stand in module for integration development between IO and the Grid module
 */
public class Fetcher
{
    private Fetcher()
    {
    }

    public static Response getData(Request request) throws IOException
    {
        GriddedReader griddedReader = new GriddedReader( request );
        try
        {
            return griddedReader.getData();
        }
        catch ( InvalidRangeException e )
        {
            throw new IOException( "Data could not be retrieved from one or more NetCDF files", e );
        }
    }

    public static Request prepareRequest()
    {
        return new GridDataRequest();
    }
}
