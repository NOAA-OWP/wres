package wres.reading.wrds.ahps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * A forecast request.
 */

@JsonIgnoreProperties( ignoreUnknown = true )
public class ForecastRequest
{
    private String url;
    private String path;

    /**
     * @return the URL
     */
    public String getUrl()
    {
        return this.url;
    }

    /**
     * Sets the URL.
     * @param url the URL
     */
    public void setUrl( String url )
    {
        this.url = url;
    }

    /**
     * @return the path
     */
    public String getPath()
    {
        return path;
    }

    /**
     * Sets the path.
     * @param path the path
     */
    public void setPath( String path )
    {
        this.path = path;
    }
}
