package wres.http;

import java.time.Duration;
import java.util.List;

import okhttp3.OkHttpClient;

public class WebClientUtils
{

    private static final List<Integer> DEFAULT_RETRY_STATES = List.of( 500,
                                                                       502,
                                                                       503,
                                                                       504,
                                                                       523,
                                                                       524 );

    /**
     * private constructor for static util method
     */
    private WebClientUtils()
    {

    }

    /**
     * Creates a base level HttpClient, is public to reduce code repeating
     * @return an OkHttpClient
     */
    public static OkHttpClient defaultHttpClient()
    {
        return new OkHttpClient().newBuilder()
                                 .followRedirects( true )
                                 .pingInterval( Duration.ofSeconds( 10 ) )
                                 .connectTimeout( Duration.ofMinutes( 1 ) )
                                 .build();
    }

    /**
     * When reading data from a source we want to limit how much time the connection remains active to avoid
     * holding up resources when sources we are reading from are unreasonably slow
     * @return an OkHttpClient
     */
    public static OkHttpClient defaultTimeoutHttpClient()
    {
        return defaultHttpClient().newBuilder()
                                  .callTimeout( Duration.ofMinutes( 20 ) )
                                  .readTimeout( Duration.ofMinutes( 20 ) )
                                  .build();
    }

    /**
     * When posting an evaluation there are periods of silence from the server, remove the default read/write timeouts
     * @return an OkHttpClient
     */
    public static OkHttpClient noTimeoutHttpClient()
    {
        return defaultHttpClient().newBuilder()
                                  .writeTimeout( Duration.ZERO )
                                  .readTimeout( Duration.ZERO )
                                  .build();
    }

    public static List<Integer> getDefaultRetryStates()
    {
        return DEFAULT_RETRY_STATES;
    }
}
