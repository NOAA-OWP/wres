package wres.http;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import okhttp3.ResponseBody;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows caller to get an InputStream from a URI with retry with exponential
 * backoff, as well as returning the HTTP status code for source-specific
 * handling (e.g. WRDS response 400 treated differently by caller vs USGS 400).
 */

public class WebClient
{
    // Have OkHttpClient print stack traces where resources are not closed.
    static
    {
        java.util.logging.Logger.getLogger( OkHttpClient.class
                                                    .getName() )
                                .setLevel( Level.FINE );
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( WebClient.class );
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds( 10 );
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes( 20 );

    // Maybe retry should be hard-coded to something, but it seems like at least
    // one connection timeout and one request timeout should be tolerated, thus
    // the need to add the two timeouts.
    private static final Duration MAX_RETRY_DURATION = CONNECT_TIMEOUT.plus( REQUEST_TIMEOUT )
                                                                      .multipliedBy( 2 );
    private static final List<Integer> DEFAULT_RETRY_STATES = List.of( 500,
                                                                       502,
                                                                       503,
                                                                       504,
                                                                       523,
                                                                       524 );
    private final OkHttpClient httpClient;
    private final boolean trackTimings;
    private final List<TimingInformation> timingInformation = new ArrayList<>( 1 );
    private String userAgent;

    /**
     * Creates an instance.
     * @param trackTimings whether to track the timings
     */
    public WebClient( boolean trackTimings )
    {
        this.httpClient = new OkHttpClient().newBuilder()
                                            .followRedirects( true )
                                            .connectTimeout( CONNECT_TIMEOUT )
                                            .callTimeout( REQUEST_TIMEOUT )
                                            .readTimeout( REQUEST_TIMEOUT )
                                            .build();
        this.trackTimings = trackTimings;
        this.setUserAgent();
    }

    /**
     * Creates an instance
     * @param sslGoo the TLS configuration
     * @param trackTimings whether to track the timings
     */
    public WebClient( Pair<SSLContext, X509TrustManager> sslGoo, boolean trackTimings )
    {
        Objects.requireNonNull( sslGoo );
        this.httpClient = new OkHttpClient().newBuilder()
                                            .followRedirects( true )
                                            .connectTimeout( CONNECT_TIMEOUT )
                                            .callTimeout( REQUEST_TIMEOUT )
                                            .readTimeout( REQUEST_TIMEOUT )
                                            .sslSocketFactory( sslGoo.getKey()
                                                                     .getSocketFactory(),
                                                               sslGoo.getRight() )
                                            .build();
        this.trackTimings = trackTimings;
        this.setUserAgent();
    }

    /**
     * Creates an instance.
     */
    public WebClient()
    {
        this( false );
    }

    /**
     * Creates an instance
     * @param sslGoo the TLS configuration
     */
    public WebClient( Pair<SSLContext, X509TrustManager> sslGoo )
    {
        this( sslGoo, false );
    }

    /**
     * @return an HTTP client
     */
    private OkHttpClient getHttpClient()
    {
        return this.httpClient;
    }

    /**
     * Get a pair of HTTP status and InputStream of body of given URI, using
     * the default retry http status codes.
     * @param uri The URI to GET and transform the body into an InputStream.
     * @return A pair of the HTTP status (left) and InputStream of body (right).
     *         NullInputStream on right when 4xx response.
     * @throws IOException When sending/receiving fails; when non-2xx non-4xx
     *                     response, when wrapping response to decompress fails.
     * @throws IllegalArgumentException When non-http uri is passed in.
     * @throws NullPointerException When any argument is null.
     */

    public ClientResponse getFromWeb( URI uri ) throws IOException
    {
        return this.getFromWeb( uri, DEFAULT_RETRY_STATES, MAX_RETRY_DURATION );
    }

    /**
     * Get a pair of HTTP status and InputStream of body of given URI, using
     * the default retry http status codes.
     * @param uri The URI to GET and transform the body into an InputStream.
     * @param timeout custom timeout for the call
     * @return A pair of the HTTP status (left) and InputStream of body (right).
     *         NullInputStream on right when 4xx response.
     * @throws IOException When sending/receiving fails; when non-2xx non-4xx
     *                     response, when wrapping response to decompress fails.
     * @throws IllegalArgumentException When non-http uri is passed in.
     * @throws NullPointerException When any argument is null.
     */

    public ClientResponse getFromWeb( URI uri, Duration timeout ) throws IOException
    {
        return this.getFromWeb( uri, DEFAULT_RETRY_STATES, timeout );
    }

    /**
     * Get a pair of HTTP status and InputStream of body of given URI.
     * @param uri The URI to GET and transform the body into an InputStream.
     * @param retryOn A list of http status codes that should cause a retry.
     * @param timeout on a call
     * @return A pair of the HTTP status (left) and InputStream of body (right).
     *         NullInputStream on right when 4xx response.
     * @throws IOException When sending/receiving fails; when non-2xx non-4xx
     *                     response, when wrapping response to decompress fails.
     * @throws IllegalArgumentException When non-http uri is passed in.
     * @throws NullPointerException When any argument is null.
     */

    public ClientResponse getFromWeb( URI uri, List<Integer> retryOn, Duration timeout )
            throws IOException
    {
        Objects.requireNonNull( uri );
        Objects.requireNonNull( retryOn );

        if ( !uri.getScheme().startsWith( "http" ) )
        {
            throw new IllegalArgumentException(
                    "Must pass an http uri, got " + uri );
        }

        LOGGER.debug( "getFromWeb {}", uri );

        WebClientEvent monitorEvent = WebClientEvent.of( uri ); // Monitor with JFR

        try
        {
            boolean retry = true;
            long sleepMillis = 1000;
            int retryCount = 0;

            Request request = new Request.Builder()
                    .url( uri.toURL() )
                    .header( "Accept-Encoding", "gzip" )
                    .header( "User-Agent", this.getUserAgent() )
                    .build();

            monitorEvent.begin();

            Instant start = Instant.now();
            Response httpResponse = tryRequest( request, retryCount );

            while ( retry )
            {
                // When a tolerable exception happened (httpResponse is null)
                // or the status is something we need to retry:
                if ( Objects.isNull( httpResponse )
                     || retryOn.contains( httpResponse.code() ) )
                {
                    if ( Objects.nonNull( httpResponse ) )
                    {
                        LOGGER.warn( "Retrying {} in a bit due to HTTP status {}.",
                                     uri,
                                     httpResponse.code() );
                    }

                    Thread.sleep( sleepMillis );
                    httpResponse = tryRequest( request, retryCount );
                    Instant now = Instant.now();
                    retry = start.plus( timeout )
                                 .isAfter( now );

                    // Exponential backoff to be nice to the server.
                    sleepMillis *= 2;
                    retryCount++;
                }
                else
                {
                    retry = false;
                }
            }

            return this.validateResponse( httpResponse, uri, retryCount, monitorEvent, start );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting data from {}", uri, ie );
            Thread.currentThread().interrupt();
            return new ClientResponse( -1 );
        }
    }

    /**
     * Launches a post request against a given URI.
     * @param uri The URI to POST
     * @param timeout the timeout of this call
     * @return A pair of the HTTP status (left) and InputStream of body (right).
     *         NullInputStream on right when 4xx response.
     * @throws IOException When sending/receiving fails; when non-2xx non-4xx
     *                     response, when wrapping response to decompress fails.
     * @throws IllegalArgumentException When non-http uri is passed in.
     * @throws NullPointerException When any argument is null.
     */

    public ClientResponse postToWeb( URI uri, Duration timeout )
            throws IOException
    {
        return postToWeb( uri, new byte[0], timeout );
    }

    /**
     * Post a pair of HTTP status and InputStream of body of given URI.
     * @param uri The URI to POST and transform the body into an InputStream.
     * @param jobMessage The body contents we want to send in a post request
     * @param timeout the timeout of this call
     * @return A pair of the HTTP status (left) and InputStream of body (right).
     *         NullInputStream on right when 4xx response.
     * @throws IOException When sending/receiving fails; when non-2xx non-4xx
     *                     response, when wrapping response to decompress fails.
     * @throws IllegalArgumentException When non-http uri is passed in.
     * @throws NullPointerException When any argument is null.
     */

    public ClientResponse postToWeb( URI uri, byte[] jobMessage, Duration timeout )
            throws IOException
    {
        Objects.requireNonNull( uri );
        Objects.requireNonNull( DEFAULT_RETRY_STATES );

        if ( !uri.getScheme().startsWith( "http" ) )
        {
            throw new IllegalArgumentException(
                    "Must pass an http uri, got " + uri );
        }

        LOGGER.debug( "getFromWeb {}", uri );

        WebClientEvent monitorEvent = WebClientEvent.of( uri ); // Monitor with JFR

        RequestBody body = RequestBody.create( jobMessage, MediaType.parse( "text/xml" ) );

        try
        {
            boolean retry = true;
            long sleepMillis = 1000;
            int retryCount = 0;

            Request request = new Request.Builder()
                    .url( uri.toURL() )
                    .post( body )
                    .header( "Content-Type", "text/xml" )
                    //                    .header( "User-Agent", this.getUserAgent() )
                    .build();


            monitorEvent.begin();

            Instant start = Instant.now();
            Response httpResponse = tryRequest( request, retryCount );

            while ( retry )
            {
                // When a tolerable exception happened (httpResponse is null)
                // or the status is something we need to retry:
                if ( Objects.isNull( httpResponse )
                     || DEFAULT_RETRY_STATES.contains( httpResponse.code() ) )
                {
                    if ( Objects.nonNull( httpResponse ) )
                    {
                        LOGGER.warn( "Retrying {} in a bit due to HTTP status {}.",
                                     uri,
                                     httpResponse.code() );
                    }

                    Thread.sleep( sleepMillis );
                    httpResponse = tryRequest( request, retryCount );
                    Instant now = Instant.now();
                    retry = start.plus( timeout )
                                 .isAfter( now );

                    // Exponential backoff to be nice to the server.
                    sleepMillis *= 2;
                    retryCount++;
                }
                else
                {
                    retry = false;
                }
            }

            return this.validateResponse( httpResponse, uri, retryCount, monitorEvent, start );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting data from {}", uri, ie );
            Thread.currentThread().interrupt();
            return new ClientResponse( -1 );
        }
    }

    /**
     * Validates the HTTP response.
     * @param httpResponse the response to validate
     * @param uri the uri requested
     * @param retryCount the number of retries
     * @param monitorEvent the monitor event
     * @param start the start time
     * @return the client response
     * @throws IOException if the client response could not be created
     */

    private ClientResponse validateResponse( Response httpResponse, URI uri,
                                             int retryCount,
                                             WebClientEvent monitorEvent,
                                             Instant start ) throws IOException
    {
        Instant end = Instant.now();
        Duration duration = Duration.between( start, end );

        monitorEvent.end(); // End, not commit

        if ( Objects.nonNull( httpResponse )
             && Objects.nonNull( httpResponse.body() ) )
        {
            int httpStatus = httpResponse.code();

            monitorEvent.setHttpResponseCode( httpStatus );
            monitorEvent.setRetryCount( retryCount );
            monitorEvent.commit();

            if ( httpStatus >= 200 && httpStatus < 300 )
            {
                LOGGER.debug( "Successfully got InputStream from {} in {}.",
                              uri,
                              duration );
            }
            else if ( httpStatus >= 400 && httpStatus < 500 )
            {
                LOGGER.debug( "Got client error from {} in {}.",
                              uri,
                              duration );
            }
            else
            {
                throw new IOException( "Failed to get data from "
                                       + uri
                                       + " due to status code "
                                       + httpStatus
                                       + " after "
                                       + duration );
            }
        }
        else
        {
            throw new IOException( "Failed to get data from "
                                   + uri
                                   + " due to repeated failures (see "
                                   + "WARN messages above) after "
                                   + duration );
        }

        return new ClientResponse( httpResponse );
    }

    /**
     *
     * @param request The request to attempt
     * @param retryCount The number of times this request has been retried
     * @return The response if successful, null if retriable.
     */

    private Response tryRequest( Request request, int retryCount )
    {
        HttpUrl uri = request.url();
        TimingInformation timing = new TimingInformation( uri );
        Response httpResponse = null;

        try
        {
            httpResponse = this.getHttpClient()
                               .newCall( request )
                               .execute();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "For {}, request headers are {} response headers are {}",
                              uri,
                              request.headers(),
                              httpResponse.headers() );
            }
        }
        catch ( IOException ioe )
        {
            LOGGER.debug( "Full exception trace: ", ioe );
            // Examine the exception chain to see if it is recoverable.
            if ( WebClient.shouldRetryWithChain( ioe, retryCount ) )
            {
                LOGGER.warn( "Retrying {} in a bit due to {}.", uri, ioe.toString() );
            }
            //            else
            //            {
            //                // Unrecoverable. If truly recoverable, add code to the method
            //                // called shouldRetryIndividualException().
            //                throw new Exception( "Unrecoverable exception when getting data from "
            //                                           + uri,
            //                                           ioe );
            //            }
        }
        finally
        {
            timing.stop();
        }

        if ( this.trackTimings )
        {
            this.timingInformation.add( timing );
        }

        return httpResponse;
    }

    private void setUserAgent()
    {
        final String VERSION_UNKNOWN = "unspecified";
        Package toGetVersion = this.getClass().getPackage();
        String wresIoVersion;

        if ( toGetVersion != null
             && toGetVersion.getImplementationVersion() != null )
        {
            // When running from a released zip, the version should show up.
            wresIoVersion = toGetVersion.getImplementationVersion();
        }
        else
        {
            // When running from source, this will be the expected outcome.
            wresIoVersion = VERSION_UNKNOWN;
        }

        this.userAgent = "wres-io/" + wresIoVersion;
    }

    private String getUserAgent()
    {
        return this.userAgent;
    }

    /**
     * Decode an HttpResponse<InputStream> based on encoding in the header.
     * Only supports empty/non-existent encoding and gzip encoding.
     * In other words, unwrap gzipped HTTP responses.
     * Credit:
     * <a href="https://stackoverflow.com/questions/53502626/does-java-http-client-handle-compression#answer-54064189">Compression</a>
     * @param response The HttpResponse having an InputStream.
     * @return An InputStream ready for consumption.
     * @throws IOException When creation of an underlying GZIPInputStream fails.
     * @throws UnsupportedOperationException When encoding is neither blank nor gzip.
     */

    private static InputStream getDecodedInputStream( Response response )
            throws IOException
    {
        Objects.requireNonNull( response );
        ResponseBody body = response.body();
        Objects.requireNonNull( body );
        InputStream rawStream = body.byteStream();
        String encoding = response.headers()
                                  .get( "Content-Encoding" );

        if ( Objects.isNull( encoding ) )
        {
            return rawStream;
        }
        else if ( encoding.equals( "gzip" ) )
        {
            return new GZIPInputStream( rawStream );
        }
        else
        {
            throw new UnsupportedOperationException( "Could not handle Content-Encoding "
                                                     + encoding );
        }
    }


    /**
     * Given an IOException, test if this one or its causes should be retried.
     * @param ioe The exception to examine
     * @param retryCount The number of times this request has been retried
     * @return True when it can be retried, false otherwise.
     */
    private static boolean shouldRetryWithChain( IOException ioe, int retryCount )
    {
        if ( WebClient.shouldRetryIndividualException( ioe, retryCount ) )
        {
            return true;
        }

        Throwable cause = ioe.getCause();

        while ( !Objects.isNull( cause ) )
        {
            if ( WebClient.shouldRetryIndividualException( cause, retryCount ) )
            {
                return true;
            }

            cause = cause.getCause();
        }

        return false;
    }

    /**
     * Look at an individual exception, return true if it ought to be retried.
     * @param t The Exception (Throwable to avoid casting mess)
     * @param retryCount The number of times the request with exception T has been retried
     * @return true when the exception can be safely retried, false otherwise.
     */
    private static boolean shouldRetryIndividualException( Throwable t, int retryCount )
    {
        if ( t instanceof HttpTimeoutException )
        {
            return true;
        }

        if ( t instanceof ConnectException )
        {
            return true;
        }

        if ( t instanceof SocketException )
        {
            return true;
        }

        if ( t instanceof SocketTimeoutException )
        {
            return true;
        }

        if ( t instanceof UnknownHostException && retryCount > 0 )
        {
            return true;
        }

        if ( t instanceof EOFException )
        {
            return true;
        }

        if ( t instanceof InterruptedIOException
             && Objects.nonNull( t.getMessage() )
             && t.getMessage()
                 .toLowerCase()
                 .contains( "timeout" ) )
        {
            return true;
        }

        return t instanceof IOException
               && Objects.nonNull( t.getMessage() )
               && t.getMessage()
                   .toLowerCase()
                   .contains( "connection reset" );
    }


    private static final class TimingInformation
    {
        private final HttpUrl uri;
        private final long startNanos;
        private long endNanos = Long.MAX_VALUE;

        /**
         * Construction starts the timer.
         * @param uri The URI being timed.
         */
        TimingInformation( HttpUrl uri )
        {
            this.uri = uri;
            this.startNanos = System.nanoTime();
        }

        void stop()
        {
            this.endNanos = System.nanoTime();
        }

        long getFullDurationNanos()
        {
            return this.endNanos - this.startNanos;
        }

        HttpUrl getUri()
        {
            return this.uri;
        }

    }

    /**
     * Return timing data collected thusfar.
     * @return The list of timings.
     */
    public String getTimingInformation()
    {
        if ( !this.trackTimings )
        {
            throw new IllegalStateException( "Cannot get timing information when timing was not requested." );
        }

        long[] timings = new long[this.timingInformation.size()];

        long min = Long.MAX_VALUE;
        TimingInformation quickest = null;
        long max = Long.MIN_VALUE;
        TimingInformation slowest = null;

        for ( int i = 0; i < this.timingInformation.size(); i++ )
        {
            TimingInformation information = this.timingInformation.get( i );
            long duration = information.getFullDurationNanos();
            timings[i] = duration;

            if ( duration < min )
            {
                min = duration;
                quickest = information;
            }

            if ( duration > max )
            {
                max = duration;
                slowest = information;
            }
        }

        Arrays.sort( timings );

        // Integer division on purpose.
        int medianIndex = timings.length / 2;
        long medianTiming = 0;

        // #77007
        if ( timings.length > 0 )
        {
            medianTiming = timings[medianIndex];
        }

        String slowestMessage = "slowest response was " + Duration.ofNanos( max );

        if ( Objects.nonNull( slowest ) )
        {
            slowestMessage += " from " + slowest.getUri();
        }

        String quickestMessage = "quickest response was " + Duration.ofNanos( min );

        if ( Objects.nonNull( quickest ) )
        {
            quickestMessage += " from " + quickest.getUri();
        }

        int countOfResponses = timings.length;

        return "Out of request/response count " + countOfResponses
               + ", median response was "
               + Duration.ofNanos( medianTiming )
               + ", "
               + quickestMessage
               + " and "
               + slowestMessage;
    }

    /**
     * A client response.
     */
    public static class ClientResponse implements AutoCloseable
    {
        private final int statusCode;
        private final InputStream response;

        /**
         * Creates an instance.
         * @param httpResponse the response code
         * @throws IOException if the client could not be created
         */
        private ClientResponse( Response httpResponse ) throws IOException
        {
            this.statusCode = httpResponse.code();
            this.response = WebClient.getDecodedInputStream( httpResponse );
        }

        /**
         * Creates an instance.
         * @param statusCode the status code
         */
        private ClientResponse( int statusCode )
        {
            this.statusCode = statusCode;
            this.response = InputStream.nullInputStream();
        }

        /**
         * @return the HTTP status code
         */

        public int getStatusCode()
        {
            return this.statusCode;
        }

        /**
         * @return the response stream
         */
        public InputStream getResponse()
        {
            return this.response;
        }

        @Override
        public void close() throws IOException
        {
            if ( this.response != null )
            {
                this.response.close();
            }
        }
    }
}
