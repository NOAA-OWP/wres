package wres.io.reading;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.SSLContext;

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
    private static final Logger LOGGER = LoggerFactory.getLogger( WebClient.class );
    private static final Duration MAX_RETRY_DURATION = Duration.ofSeconds( 30 );
    private static final List<Integer> DEFAULT_RETRY_STATI = List.of( 500,
                                                                      502,
                                                                      503,
                                                                      504,
                                                                      523,
                                                                      524 );

    private final HttpClient httpClient;

    public WebClient()
    {
        this.httpClient = HttpClient.newBuilder()
                                    .followRedirects( HttpClient.Redirect.NORMAL )
                                    .build();
    }

    public WebClient( SSLContext sslContext )
    {
        Objects.requireNonNull( sslContext );
        this.httpClient = HttpClient.newBuilder()
                                    .followRedirects( HttpClient.Redirect.NORMAL )
                                    .sslContext( sslContext )
                                    .build();
    }

    private HttpClient getHttpClient()
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

    public Pair<Integer, InputStream> getFromWeb( URI uri ) throws IOException
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

    public Pair<Integer, InputStream> getFromWeb( URI uri,
                                                  List<Integer> retryOn )
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

        try
        {
            HttpRequest request = HttpRequest.newBuilder()
                                             .uri( uri )
                                             .header( "Accept-Encoding", "gzip" )
                                             .build();
            HttpResponse<InputStream> httpResponse =
                    this.getHttpClient()
                        .send( request,
                               HttpResponse.BodyHandlers.ofInputStream() );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "For {}, request headers are {} response headers are {}",
                              uri,
                              request.headers(),
                              httpResponse.headers() );
            }

            int httpStatus = httpResponse.statusCode();

            boolean retry = true;
            Instant start = Instant.now();
            long sleepMillis = 1000;

            while ( retry )
            {
                if ( retryOn.contains( httpStatus ) )
                {
                    LOGGER.warn( "Retrying {} in a bit due to http status {}.",
                                 uri, httpStatus );
                    Thread.sleep( sleepMillis );
                    httpResponse = this.getHttpClient()
                                       .send( request,
                                              HttpResponse.BodyHandlers.ofInputStream() );
                    httpStatus = httpResponse.statusCode();
                    Instant now = Instant.now();
                    retry = start.plus( MAX_RETRY_DURATION )
                                 .isAfter( now );

                    // Exponential backoff to be nice to the server.
                    sleepMillis *= 2;
                }
                else
                {
                    retry = false;
                }
            }

            Instant end = Instant.now();
            Duration duration = Duration.between( start, end );

            if ( httpStatus >= 200 && httpStatus < 300 )
            {
                LOGGER.debug( "Successfully got InputStream from {} in {}", uri,
                              duration );
                InputStream decodedStream = WebClient.getDecodedInputStream( httpResponse );
                return Pair.of( httpStatus, decodedStream );
            }
            else if ( httpStatus >= 400 && httpStatus < 500 )
            {
                LOGGER.debug( "Got empty/not-found data from {} in {}", uri,
                              duration );
                return Pair.of( httpStatus, InputStream.nullInputStream() );
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
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting data from {}", uri, ie );
            Thread.currentThread().interrupt();
            return Pair.of( -1, InputStream.nullInputStream() );
        }
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

    private static InputStream getDecodedInputStream( HttpResponse<InputStream> response )
            throws IOException
    {
        InputStream rawStream = response.body();
        String encoding = response.headers()
                                  .firstValue( "Content-Encoding" )
                                  .orElse( "" );

        if ( encoding.equals( "" ) )
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
}
