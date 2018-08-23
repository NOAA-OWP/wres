package wres.io.reading.wrds;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ForecastResponse
{
    public short getStatusCode()
    {
        return status_code;
    }

    public String getMesssage()
    {
        return messsage;
    }

    public Header getHeader()
    {
        return header;
    }

    public void setHeader( Header header )
    {
        this.header = header;
    }

    public void setMesssage( String messsage )
    {
        this.messsage = messsage;
    }

    public void setStatus_code( short status_code )
    {
        this.status_code = status_code;
    }

    public Forecast[] getForecasts()
    {
        return forecasts;
    }

    public void setForecasts( Forecast[] forecasts )
    {
        this.forecasts = forecasts;
    }

    short status_code;
    String messsage;
    Header header;
    Forecast[] forecasts;
}
