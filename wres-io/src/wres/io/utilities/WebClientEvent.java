package wres.io.utilities;

import java.net.URI;
import java.util.Objects;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.Threshold;

/**
 * A custom event for monitoring the reading of time-series data from web services, exposed to the Java Flight Recorder.
 * 
 * @author james.brown@hydrosolved.com
 */

@Name( "wres.io.utilities.WebClientEvent" )
@Label( "Web Resource Event" )
@Category( { "Java Application", "Water Resources Evaluation Service", "Core client", "Reading" } )
@Threshold( "10000 ms" )
class WebClientEvent extends Event
{
    @Label( "Resource URI" )
    @Description( "The URI of the resource that was requested." )
    private final String uri;

    @Label( "HTTP Response Code" )
    @Description( "The HTTP response code received from the web service." )
    private int httpResponseCode;

    @Label( "Retry Count" )
    @Description( "The number of retries attemped. Zero means the request completed (succeeded or failed, terminally) "
                  + "on first attempt." )
    private int retryCount;

    /**
     * @param uri the URI of the web resource
     * @return an instance
     * @throws NullPointerException if the uri is null
     */

    static WebClientEvent of( URI uri )
    {
        return new WebClientEvent( uri );
    }

    /**
     * Sets the response code.
     * @param httpResponseCode the http response code
     */

    void setHttpResponseCode( int httpResponseCode )
    {
        this.httpResponseCode = httpResponseCode;
    }

    /**
     * Sets the retry count.
     * @param retryCount the retry count
     */

    void setRetryCount( int retryCount )
    {
        this.retryCount = retryCount;
    }

    /**
     * Hidden constructor.
     * @param uri the URI of the web resource
     * @throws NullPointerException if the uri is null
     */

    private WebClientEvent( URI uri )
    {
        Objects.requireNonNull( uri );

        this.uri = uri.toString();
    }
}
