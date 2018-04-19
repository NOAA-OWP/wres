package wres.grid.client;

import java.io.IOException;

public class Fetcher
{
    public static Response getData(Request request) throws IOException
    {
        TimeSeriesResponse response = Fetcher.fakeOutResponseData(request);


        return response;
    }

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
