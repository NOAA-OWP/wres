package wres.grid.client;

import java.io.IOException;

/**
 * Stand in module for integration development between IO and the Grid module
 */
public class Fetcher
{
    public static Response getData(Request request) throws IOException
    {
        TimeSeriesResponse response = Fetcher.fakeOutResponseData(request);


        return response;
    }

    /**
     * Stand-in function for creating fake response data
     * @param request
     * @return A response object with fake data
     */
    private static TimeSeriesResponse fakeOutResponseData(Request request)
    {
        TimeSeriesResponse response = new TimeSeriesResponse();



        return response;
    }

    public static Request prepareRequest()
    {
        return new GridDataRequest();
    }
}
