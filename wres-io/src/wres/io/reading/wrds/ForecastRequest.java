package wres.io.reading.wrds;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties( ignoreUnknown = true )
public class ForecastRequest
{
    public String getUrl()
    {
        return this.url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getPath()
    {
        return path;
    }

    public void setPath(String path)
    {
        this.path = path;
    }

    String url;
    String path;
}
