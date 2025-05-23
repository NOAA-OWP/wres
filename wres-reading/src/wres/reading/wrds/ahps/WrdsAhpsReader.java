package wres.reading.wrds.ahps;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import okhttp3.OkHttpClient;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.http.WebClientUtils;
import wres.reading.PreReadException;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.http.WebClient;
import wres.statistics.generated.GeometryTuple;
import wres.system.SystemSettings;

/**
 * Reads time-series data from the National Weather Service (NWS) Water Resources Data Service for the Advanced
 * Hydrologic Prediction Service (AHPS). The service requests are chunked into year ranges for observations or
 * simple ranges, based on the declaration, for forecast sources. The underlying format reader is a
 * {@link WrdsAhpsJsonReader}.
 *
 * @author James Brown
 */

public class WrdsAhpsReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsAhpsReader.class );

    /** The underlying format reader for JSON-formatted data from the AHPS service. */
    private static final WrdsAhpsJsonReader AHPS_READER = WrdsAhpsJsonReader.of();

    /** Forward slash character. */
    private static final String SLASH = "/";

    /** Message string. */
    private static final String WRDS_AHPS = "WRDS AHPS";

    /** Custom HttpClient to use */
    private static final WebClient CUSTOM_WEB_CLIENT;

    /** The HTTP response codes considered to represent no data. Is this too broad? Perhaps a 404 only, else a read
     * exception. The problem is that "no data" is routine from the perspective of WRES, but apparently not WRDS, so we
     * get a 404, not a 200. See Redmine issue #116808. The difficulty with such a broad range is that we potentially
     * aggregate buggy requests with no data responses. */
    private static final IntPredicate NO_DATA_PREDICATE = h -> h >= 400 && h < 500;

    /** The HTTP response codes considered to represent an error to be thrown. */
    private static final IntPredicate ERROR_RESPONSE_PREDICATE = h -> h >= 500;

    /** Pair declaration, which is used to chunk requests. Null if no chunking is required. */
    private final EvaluationDeclaration declaration;

    /** A thread pool to process web requests. */
    private final ThreadPoolExecutor executor;

    static
    {
        try
        {
            Pair<SSLContext, X509TrustManager> sslContext = ReaderUtilities.getSslContextForWrds();
            OkHttpClient client = WebClientUtils.defaultTimeoutHttpClient()
                                                .newBuilder()
                                                .sslSocketFactory( sslContext.getKey().getSocketFactory(),
                                                                   sslContext.getRight() )
                                                .build();
            CUSTOM_WEB_CLIENT = new WebClient( client );
        }
        catch ( PreReadException e )
        {
            throw new ExceptionInInitializerError( "Failed to acquire the TLS context for connecting to WRDS: "
                                                   + e.getMessage() );
        }
    }

    /**
     * @see #of(EvaluationDeclaration, SystemSettings)
     * @param systemSettings the system settings
     * @return an instance that does not perform any chunking of the time-series data
     * @throws NullPointerException if the systemSettings is null
     */

    public static WrdsAhpsReader of( SystemSettings systemSettings )
    {
        return new WrdsAhpsReader( null, systemSettings );
    }

    /**
     * @param declaration the declaration, which is used to perform chunking of a data source
     * @param systemSettings the system settings
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    public static WrdsAhpsReader of( EvaluationDeclaration declaration, SystemSettings systemSettings )
    {
        Objects.requireNonNull( declaration );

        return new WrdsAhpsReader( declaration, systemSettings );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Chunk the requests if needed
        if ( Objects.nonNull( this.getDeclaration() ) )
        {
            LOGGER.debug( "Preparing requests for WRDS AHPS time-series that chunk the time-series data by feature and "
                          + "time range." );
            return this.read( dataSource, this.getDeclaration() );
        }

        LOGGER.debug( "Preparing a request to WRDS for AHPS time-series without any chunking of the data." );
        InputStream stream = ReaderUtilities.getByteStreamFromWebSource( dataSource.getUri(),
                                                                         NO_DATA_PREDICATE,
                                                                         ERROR_RESPONSE_PREDICATE,
                                                                         null,
                                                                         CUSTOM_WEB_CLIENT );

        if ( Objects.isNull( stream ) )
        {
            LOGGER.warn( "Failed to obtain time-series data from {}. Returning an empty stream.", dataSource.getUri() );

            return Stream.of();
        }

        return this.read( dataSource, stream );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream stream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( stream );

        this.validateSource( dataSource );

        LOGGER.debug( "Discovered an existing stream, assumed to be from a WRDS AHPS service instance. Passing through "
                      + "to an underlying WRDS AHPS JSON reader." );

        return AHPS_READER.read( dataSource, stream );
    }

    /**
     * @return the declaration, possibly null
     */

    private EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
    }

    /**
     * @return the thread pool executor
     */

    private ThreadPoolExecutor getExecutor()
    {
        return this.executor;
    }

    /**
     * Reads the data source by forming separate requests by feature and time range.
     *
     * @param dataSource the data source
     * @param declaration the declaration used for chunking
     * @throws NullPointerException if either input is null
     */

    private Stream<TimeSeriesTuple> read( DataSource dataSource, EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( declaration );

        this.validateSource( dataSource );

        // The features
        Set<GeometryTuple> geometries = DeclarationUtilities.getFeatures( declaration );
        Set<String> features = ReaderUtilities.getFeatureNamesFor( geometries, dataSource );

        // Date ranges
        Set<Pair<Instant, Instant>> dateRanges;
        if ( ReaderUtilities.isWrdsObservedSource( dataSource ) )
        {
            dateRanges = ReaderUtilities.getYearRanges( declaration, dataSource );
        }
        else
        {
            dateRanges = new HashSet<>();
            Pair<Instant, Instant> range = ReaderUtilities.getSimpleRange( declaration, dataSource );
            dateRanges.add( range );
        }

        // Combine the features and date ranges to form the chunk boundaries
        Set<Pair<String, Pair<Instant, Instant>>> chunks = new HashSet<>();
        for ( String nextFeature : features )
        {
            for ( Pair<Instant, Instant> nextDates : dateRanges )
            {
                Pair<String, Pair<Instant, Instant>> nextChunk = Pair.of( nextFeature, nextDates );
                chunks.add( nextChunk );
            }
        }

        // Get the lazy supplier of time-series data, which supplies one series per chunk of data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource,
                                                                         Collections.unmodifiableSet( chunks ) );

        // Generate a stream of time-series. Nothing is read here. Rather, as part of a terminal operation on this
        // stream, each pull will read through to the supplier, then in turn to the data provider, and finally to
        // the data source.
        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event. Closing dependent resources." );
                         this.getExecutor()
                             .shutdownNow();
                     } );
    }

    /**
     * Returns a time-series supplier from the inputs.
     *
     * @param dataSource the data source
     * @param chunks the data chunks to iterate
     * @return a time-series supplier
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             Set<Pair<String, Pair<Instant, Instant>>> chunks )
    {
        LOGGER.debug( "Creating a time-series supplier to supply one time-series for each of these {} chunks: {}.",
                      chunks.size(),
                      chunks );

        SortedSet<Pair<String, Pair<Instant, Instant>>> mutableChunks = new TreeSet<>( chunks );

        // The size of this queue is equal to the setting for simultaneous web client threads so that we can 1. get
        // quick feedback on exception (which requires a small queue) and 2. allow some requests to go out prior to
        // get-one-response-per-submission-of-one-ingest-task
        int concurrentCount = this.getExecutor()
                                  .getMaximumPoolSize();
        BlockingQueue<Future<List<TimeSeriesTuple>>> results = new ArrayBlockingQueue<>( concurrentCount );

        // The size of this latch is for reason (2) above
        CountDownLatch startGettingResults = new CountDownLatch( concurrentCount );

        // Cached time-series to return
        List<TimeSeriesTuple> cachedSeries = new ArrayList<>();

        // Is true to continue looking for time-series
        AtomicBoolean proceed = new AtomicBoolean( true );

        // Create a supplier that returns a time-series once complete
        return () -> {

            // Clean up before sending the null sentinel, which terminates the stream
            // New rows to increment
            while ( proceed.get() )
            {
                // Cached series from an earlier iteration? If so, return it
                if ( !cachedSeries.isEmpty() )
                {
                    return cachedSeries.remove( 0 );
                }

                // Submit the next chunk if not already submitted
                if ( !mutableChunks.isEmpty() )
                {
                    Pair<String, Pair<Instant, Instant>> nextChunk = mutableChunks.first();
                    mutableChunks.remove( nextChunk );

                    // Create the inner data source for the chunk
                    URI nextUri = this.getUriForChunk( dataSource.getSource()
                                                                 .uri(),
                                                       nextChunk.getRight(),
                                                       nextChunk.getLeft(),
                                                       dataSource.getSource()
                                                                 .parameters(),
                                                       ReaderUtilities.isWrdsObservedSource( dataSource ) );

                    DataSource innerSource =
                            DataSource.of( dataSource.getDisposition(),
                                           dataSource.getSource(),
                                           dataSource.getContext(),
                                           dataSource.getLinks(),
                                           nextUri,
                                           dataSource.getDatasetOrientation(),
                                           dataSource.getCovariateFeatureOrientation() );

                    LOGGER.debug( "Created data source for chunk, {}.", innerSource );

                    // Get the next time-series as a future
                    Future<List<TimeSeriesTuple>> future = this.getTimeSeriesTuple( innerSource );

                    results.add( future );
                }

                // Check that all is well with previously submitted tasks, but only after a handful have been
                // submitted. This means that an exception should propagate relatively shortly after it occurs with the
                // read task. It also means after the creation of a handful of tasks, we only create one after a
                // previously created one has been completed, fifo/lockstep.
                startGettingResults.countDown();
                List<TimeSeriesTuple> result = ReaderUtilities.getTimeSeries( results,
                                                                              startGettingResults,
                                                                              WRDS_AHPS );

                cachedSeries.addAll( result );

                // Still some chunks to request or results to return, bearing in mind that one will be returned below?
                proceed.set( !mutableChunks.isEmpty() || !results.isEmpty() || cachedSeries.size() > 1 );

                LOGGER.debug( "Continuing to iterate chunks of data ({}) because some chunks were yet to be submitted "
                              + "({}) or some results were yet to be retrieved ({}) or some results are cached and "
                              + "awaiting return ({}).",
                              proceed.get(),
                              !mutableChunks.isEmpty(),
                              !results.isEmpty(),
                              cachedSeries.size() > 1 );

                // Return a result if there is one
                if ( !cachedSeries.isEmpty() )
                {
                    return cachedSeries.remove( 0 );
                }
            }

            // Null sentinel to close stream
            return null;
        };
    }

    /**
     * @param dataSource the data source
     * @return a time-series task
     */

    private Future<List<TimeSeriesTuple>> getTimeSeriesTuple( DataSource dataSource )
    {
        LOGGER.debug( "Submitting a task for retrieving a time-series." );

        return this.getExecutor()
                   .submit( () -> {
                       // Get the input stream and read from it
                       try ( InputStream inputStream = ReaderUtilities.getByteStreamFromWebSource( dataSource.getUri(),
                                                                                                   NO_DATA_PREDICATE,
                                                                                                   ERROR_RESPONSE_PREDICATE,
                                                                                                   null,
                                                                                                   CUSTOM_WEB_CLIENT ) )
                       {
                           if ( Objects.isNull( inputStream ) )
                           {
                               LOGGER.warn( "Failed to obtain time-series data from {}. Returning an empty stream.",
                                            dataSource.getUri() );

                               return List.of();
                           }

                           try ( Stream<TimeSeriesTuple> seriesStream = AHPS_READER.read( dataSource, inputStream ) )
                           {
                               return seriesStream.toList(); // Terminal
                           }
                       }
                   } );
    }

    /**
     * @param dataSource the data source
     * @throws ReadException if the source is invalid
     */

    private void validateSource( DataSource dataSource )
    {
        if ( !( ReaderUtilities.isWrdsAhpsSource( dataSource )
                || ReaderUtilities.isWrdsObservedSource( dataSource ) ) )
        {
            throw new ReadException( "Expected a WRDS AHPS data source, but got: " + dataSource + "." );
        }
    }

    /**
     * Gets a URI for given date range and feature.
     *
     * <p>Expecting a wrds URI like this:
     * <a href="http://redacted/api/v1/forecasts/streamflow/ahps">http://redacted/api/v1/forecasts/streamflow/ahps</a></p>
     * @param baseUri the base URI
     * @param range the range of dates (from left to right)
     * @param nwsLocationId the feature for which to get data
     * @param additionalParameters the additional parameters, if any
     * @param observed true for observations, false for AHPS forecasts
     * @return a URI suitable to get the data from WRDS API
     * @throws ReadException if the URI could not be constructed
     */

    private URI getUriForChunk( URI baseUri,
                                Pair<Instant, Instant> range,
                                String nwsLocationId,
                                Map<String, String> additionalParameters,
                                boolean observed )
    {
        String basePath = baseUri.getPath();

        // Tolerate either a slash at end or not.
        if ( !basePath.endsWith( SLASH ) )
        {
            basePath = basePath + SLASH;
        }

        // Add nws_lid to the end of the path.
        // TODO Remove the outer if-check once the old, 1.1 API is gone.
        if ( !basePath.contains( "v1.1" )
             && !basePath.endsWith( "nws_lid/" ) )
        {
            basePath = basePath + "nws_lid/";
        }

        Map<String, String> wrdsParameters = this.createWrdsAhpsUrlParameters( range,
                                                                               additionalParameters,
                                                                               observed );
        String pathWithLocation = basePath
                                  + nwsLocationId;
        URIBuilder uriBuilder = new URIBuilder( baseUri );
        uriBuilder.setPath( pathWithLocation );

        URI uriWithLocation;
        try
        {
            uriWithLocation = uriBuilder.build();
        }
        catch ( URISyntaxException use )
        {
            throw new ReadException( "Could not create a URI from "
                                     + baseUri
                                     + " and "
                                     + pathWithLocation
                                     + ".",
                                     use );
        }

        return ReaderUtilities.getUriWithParameters( uriWithLocation,
                                                     wrdsParameters );
    }

    /**
     * Specific to WRDS API, get date range url parameters
     * @param dateRange the date range to set parameters for
     * @param additionalParameters the additional parameters, if any
     * @param observed is true if the target dataset contains observations
     * @return the key/value parameters
     */

    private Map<String, String> createWrdsAhpsUrlParameters( Pair<Instant, Instant> dateRange,
                                                             Map<String, String> additionalParameters,
                                                             boolean observed )
    {
        Map<String, String> urlParameters = new HashMap<>( 2 );

        // Set the proj field, allowing for a user to override it with a URL
        // parameter, which is handled next.
        urlParameters.put( "proj", ReaderUtilities.DEFAULT_WRDS_PROJ );

        // Caller-supplied additional parameters are lower precedence, put first
        urlParameters.putAll( additionalParameters );

        String timeTag = "issuedTime";
        if ( observed )
        {
            timeTag = "validTime";
        }

        urlParameters.put( timeTag,
                           "[" + dateRange.getLeft()
                                          .toString()
                           + ","
                           + dateRange.getRight()
                                      .toString()
                           + "]" );

        return Collections.unmodifiableMap( urlParameters );
    }

    /**
     * Hidden constructor.
     * @param declaration the optional pair declaration, which is used to perform chunking of a data source
     * @param systemSettings the system settings, required
     * @throws wres.config.yaml.DeclarationException if the project declaration is invalid for this source type
     * @throws NullPointerException if the systemSettings is null
     */

    private WrdsAhpsReader( EvaluationDeclaration declaration, SystemSettings systemSettings )
    {
        Objects.requireNonNull( systemSettings );

        this.declaration = declaration;

        ThreadFactory webClientFactory = new BasicThreadFactory.Builder().namingPattern( "WRDS AHPS Reading Thread %d" )
                                                                         .build();

        // Use a queue with as many places as client threads
        BlockingQueue<Runnable> webClientQueue =
                new ArrayBlockingQueue<>( systemSettings.getMaximumWebClientThreads() );
        this.executor = new ThreadPoolExecutor( systemSettings.getMaximumWebClientThreads(),
                                                systemSettings.getMaximumWebClientThreads(),
                                                systemSettings.getPoolObjectLifespan(),
                                                TimeUnit.MILLISECONDS,
                                                webClientQueue,
                                                webClientFactory );

        // Because of use of latch and queue below, rejection should not happen.
        this.executor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
    }
}
