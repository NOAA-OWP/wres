package wres.io.reading.wrds;

public class Header
{
    public ForecastRequest getRequest()
    {
        return request;
    }

    public void setRequest( ForecastRequest request )
    {
        this.request = request;
    }

    ForecastRequest request;
}
