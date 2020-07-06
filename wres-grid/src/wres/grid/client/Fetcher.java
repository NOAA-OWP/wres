package wres.grid.client;

import java.io.IOException;
import java.util.List;

import wres.config.generated.Feature;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeWindowOuter;
import wres.grid.reading.GriddedReader;

/**
 * Stand in module for integration development between IO and the Grid module
 */
public class Fetcher
{
    /**
     * Returns a single-valued time-series response from the request.
     * 
     * @param request the request
     * @return the single-value response
     * @throws IOException if the gridded values could not be read for any reason
     */

    public static SingleValuedTimeSeriesResponse getSingleValuedTimeSeries( Request request ) throws IOException
    {
        try
        {
            return GriddedReader.getSingleValuedResponse( request );
        }
        catch ( IOException e )
        {
            throw new IOException( "Data could not be retrieved from one or more NetCDF files", e );
        }
    }

    public static SingleValuedTimeSeriesResponse getSingleValuedTimeSeries() throws IOException
    {
        return null;
    }
    
    /**
     * Returns an instance.
     * 
     * @param paths the paths to read
     * @param features the features to read
     * @param variableName the variable to read
     * @param timeWindow the time window to consider
     * @param isForecast is true if the paths point to forecasts, otherwise false
     * @param declaredExistingTimeScale optional time-scale information that can augment, but not override
     * @return an instance
     * @throws NullPointerException if any nullable input is null
     */

    public static GridDataRequest prepareRequest( List<String> paths,
                                                  List<Feature> features,
                                                  String variableName,
                                                  TimeWindowOuter timeWindow,
                                                  boolean isForecast,
                                                  TimeScale declaredExistingTimeScale )
    {
        return GridDataRequest.of( paths, features, variableName, timeWindow, isForecast, declaredExistingTimeScale );
    }

    /**
     * Do not construct.
     */
    private Fetcher()
    {
    }

}
