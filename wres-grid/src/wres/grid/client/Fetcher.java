package wres.grid.client;

import java.io.IOException;
import java.util.List;

import wres.config.generated.Feature;
import wres.datamodel.time.TimeWindow;
import wres.grid.reading.GriddedReader;

/**
 * Stand in module for integration development between IO and the Grid module
 */
public class Fetcher
{

    public static Response getData( Request request ) throws IOException
    {
        GriddedReader griddedReader = new GriddedReader( request );
        try
        {
            return griddedReader.getData();
        }
        catch ( IOException e )
        {
            throw new IOException( "Data could not be retrieved from one or more NetCDF files", e );
        }
    }

    /**
     * Returns an instance.
     * 
     * @param paths the paths to read
     * @param features the features to read
     * @param variableName the variable to read
     * @param timeWindow the time window to consider
     * @param isForecast is true if the paths point to forecasts, otherwise false
     * @return an instance
     * @throws NullPointerException if any nullable input is null
     */

    public static GridDataRequest prepareRequest( List<String> paths,
                                                  List<Feature> features,
                                                  String variableName,
                                                  TimeWindow timeWindow,
                                                  boolean isForecast )
    {
        return GridDataRequest.of( paths, features, variableName, timeWindow, isForecast );
    }

    /**
     * Do not construct.
     */
    private Fetcher()
    {
    }

}
