package wres.io.utilities;

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
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.reading.IngestException;
import wres.io.reading.PreIngestException;
import wres.system.SSLStuffThatTrustsOneCertificate;

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
                                .setLevel( Level.FINE);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( WebClient.class );
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds( 10 );
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes( 20 );

    // Maybe retry should be hard-coded to something, but it seems like at least
    // one connection timeout and one request timeout should be tolerated, thus
    // the need to add the two timeouts.
    private static final Duration MAX_RETRY_DURATION = CONNECT_TIMEOUT.plus( REQUEST_TIMEOUT )
                                                                      .multipliedBy( 2 );
    private static final List<Integer> DEFAULT_RETRY_STATI = List.of( 500,
                                                                      502,
                                                                      503,
                                                                      504,
                                                                      523,
                                                                      524 );

    private final OkHttpClient httpClient;
    private final boolean trackTimings;
    private final List<TimingInformation> timingInformation = new ArrayList<>( 1 );

    public WebClient( boolean trackTimings )
    {
        this.httpClient = new OkHttpClient().newBuilder()
                                            .followRedirects( true )
                                            .connectTimeout( CONNECT_TIMEOUT )
                                            .callTimeout( REQUEST_TIMEOUT )
                                            .readTimeout( REQUEST_TIMEOUT )
                                            .build();
        this.trackTimings = trackTimings;
    }

    public WebClient( Pair<SSLContext,X509TrustManager> sslGoo, boolean trackTimings )
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
    }

    public WebClient()
    {
        this( false );
    }

    public WebClient( Pair<SSLContext,X509TrustManager> sslGoo )
    {
        this( sslGoo, false );
    }

    public static Pair<SSLContext,X509TrustManager> createSSLContext(String trustFileOnClassPath)
    {
        try ( InputStream inputStream = WebClient.class
                .getClassLoader()
                .getResourceAsStream( trustFileOnClassPath ) )
        {
            // Avoid sending null, log a warning instead, use default.
            if ( inputStream == null )
            {
                LOGGER.warn( "Failed to load {} from classpath. Using default SSLContext.",
                        trustFileOnClassPath );

                X509TrustManager theTrustManager = null;
                for ( TrustManager manager : TrustManagerFactory.getInstance( TrustManagerFactory.getDefaultAlgorithm() )
                        .getTrustManagers() )
                {
                    if ( manager instanceof X509TrustManager )
                    {
                        LOGGER.warn( "Failed to load {} from classpath. Using this X509TrustManager: {}",
                                trustFileOnClassPath, manager );
                        theTrustManager = (X509TrustManager) manager;
                    }
                }
                if ( Objects.isNull( theTrustManager) )
                {
                    throw new UnsupportedOperationException( "Could not find a default X509TrustManager" );
                }
                return Pair.of( SSLContext.getDefault(), theTrustManager );
            }
            SSLStuffThatTrustsOneCertificate sslGoo =
                    new SSLStuffThatTrustsOneCertificate( inputStream );
            return Pair.of( sslGoo.getSSLContext(), sslGoo.getTrustManager() );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Unable to read "
                    + trustFileOnClassPath
                    + " from classpath in order to add it"
                    + " to trusted certificate list for "
                    + "requests made to WRDS services.",
                    ioe );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new PreIngestException( "Unable to find "
                    + trustFileOnClassPath
                    + " on classpath in order to add it"
                    + " to trusted certificate list for "
                    + "requests made to WRDS services "
                    + "and furthermore could not get the "
                    + "default SSLContext.", nsae );
        }
    }

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
        return this.getFromWeb( uri, DEFAULT_RETRY_STATI );
    }

    /**
     * Get a pair of HTTP status and InputStream of body of given URI.
     * @param uri The URI to GET and transform the body into an InputStream.
     * @param retryOn A list of http status codes that should cause a retry.
     * @return A pair of the HTTP status (left) and InputStream of body (right).
     *         NullInputStream on right when 4xx response.
     * @throws IOException When sending/receiving fails; when non-2xx non-4xx
     *                     response, when wrapping response to decompress fails.
     * @throws IllegalArgumentException When non-http uri is passed in.
     * @throws NullPointerException When any argument is null.
     */

    public ClientResponse getFromWeb( URI uri, List<Integer> retryOn )
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
            Request request = new Request.Builder()
                    .url( uri.toURL() )
                    .header( "Accept-Encoding", "gzip" )
                    .build();

            monitorEvent.begin();
            
            Instant start = Instant.now();
            Response httpResponse = tryRequest( request );

            boolean retry = true;
            long sleepMillis = 1000;
            int retryCount = 0;

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
                    httpResponse = tryRequest( request );
                    Instant now = Instant.now();
                    retry = start.plus( MAX_RETRY_DURATION )
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

            Instant end = Instant.now();
            Duration duration = Duration.between( start, end );
            
            monitorEvent.end();  // End, not commit

            if ( Objects.nonNull( httpResponse )
                 && Objects.nonNull( httpResponse.body() ) )
            {
                int httpStatus = httpResponse.code();

                monitorEvent.setHttpResponseCode( httpStatus );
                monitorEvent.setRetryCount( retryCount );
                monitorEvent.commit();
                
                if ( httpStatus >= 200 && httpStatus < 300 )
                {
                    LOGGER.debug( "Successfully got InputStream from {} in {}",
                                  uri,
                                  duration );
                    return new ClientResponse(httpResponse);
                }
                else if ( httpStatus >= 400 && httpStatus < 500 )
                {
                    LOGGER.debug( "Got empty/not-found data from {} in {}", uri,
                                  duration );

                    httpResponse.body().close();
                    return new ClientResponse(httpStatus);
                }
                else
                {
                    throw new IngestException( "Failed to get data from "
                                               + uri
                                               + " due to status code "
                                               + httpStatus
                                               + " after " + duration );
                }
            }
            else
            {
                throw new IngestException( "Failed to get data from "
                                           + uri
                                           + " due to repeated failures (see "
                                           + "WARN messages above) after "
                                           + duration );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting data from {}", uri, ie );
            Thread.currentThread().interrupt();
            return new ClientResponse(-1);
        }
    }


    /**
     *
     * @param request The request to attempt
     * @return The response if successful, null if retriable.
     * @throws IOException When non-retriable exception occurs.
     */

    private Response tryRequest( Request request )
            throws IOException
    {
        LOGGER.debug( "Called tryRequest with {}", request );
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
            if ( WebClient.shouldRetryWithChain( ioe ) )
            {
                LOGGER.warn( "Retrying {} in a bit due to {}.", uri, ioe.toString() );
            }
            else
            {
                // Unrecoverable. If truly recoverable, add code to the method
                // called shouldRetryIndividualException().
                throw new IngestException( "Unrecoverable exception when getting data from "
                                           + uri, ioe );
            }
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

    /**
     * Decode an HttpResponse<InputStream> based on encoding in the header.
     * Only supports empty/non-existent encoding and gzip encoding.
     * In other words, unwrap gzipped HTTP responses.
     * Credit:
     * https://stackoverflow.com/questions/53502626/does-java-http-client-handle-compression#answer-54064189
     * @param response The HttpResponse having an InputStream.
     * @return An InputStream ready for consumption.
     * @throws IOException When creation of an underlying GZIPInputStream fails.
     * @throws UnsupportedOperationException When encoding is neither blank nor gzip.
     */

    private static InputStream getDecodedInputStream( Response response )
            throws IOException
    {
        InputStream rawStream = response.body()
                                        .byteStream();
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
     * @return True when it can be retried, false otherwise.
     */
    private static boolean shouldRetryWithChain( IOException ioe )
    {
        if ( WebClient.shouldRetryIndividualException( ioe ) )
        {
            return true;
        }

        Throwable cause = ioe.getCause();

        while ( !Objects.isNull( cause ) )
        {
            if ( WebClient.shouldRetryIndividualException( cause ) )
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
     * @return true when the exception can be safely retried, false otherwise.
     */
    private static boolean shouldRetryIndividualException( Throwable t )
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

        if ( t instanceof UnknownHostException )
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

        if ( t instanceof IOException
             && Objects.nonNull( t.getMessage() )
             && t.getMessage()
                 .toLowerCase()
                 .contains( "connection reset" ) )
        {
            return true;
        }

        return false;
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

        long[] timings = new long[ this.timingInformation.size() ];

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
        if( timings.length > 0 )
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
               + Duration.ofNanos( medianTiming ) + ", "
               + quickestMessage + " and "
               + slowestMessage;
    }

    public static class ClientResponse implements AutoCloseable {
        private final int statusCode;
        private final InputStream response;

        public ClientResponse(Response httpResponse) throws IOException {
            this.statusCode = httpResponse.code();
            this.response = WebClient.getDecodedInputStream( httpResponse );
        }

        public ClientResponse(int statusCode) {
            this.statusCode = statusCode;
            this.response = InputStream.nullInputStream();
        }

        public int getStatusCode() {
            return this.statusCode;
        }

        public InputStream getResponse() {
            return this.response;
        }

        @Override
        public void close() throws IOException {
            if (this.response != null) {
                this.response.close();
            }
        }
    }
}
