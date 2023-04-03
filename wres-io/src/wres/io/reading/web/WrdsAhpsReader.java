package wres.io.reading.web;

import java.io.IOException;
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
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.xml.ProjectConfigException;
import wres.config.generated.DateCondition;
import wres.config.generated.PairConfig;
import wres.config.generated.UrlParameter;
import wres.io.config.ConfigHelper;
import wres.io.ingesting.PreIngestException;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.waterml.WatermlReader;
import wres.io.reading.wrds.WrdsAhpsJsonReader;
import wres.system.SystemSettings;

/**
 * Reads time-series data from the National Weather Service (NWS) Water Resources Data Service for the Advanced 
 * Hydrologic Prediction Service (AHPS). The service requests are chunked into year ranges for observations or 
 * simple ranges, based on the declaration, for forecast sources.
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

    /** Re-used string. */
    private static final String WHEN_USING_WRDS_AS_A_SOURCE_OF_TIME_SERIES_DATA_YOU_MUST_DECLARE =
            "When using WRDS as a source of time-series data, you must declare ";

    /** Message string. */
    private static final String WRDS_AHPS = "WRDS AHPS";

    /** Trust manager for TLS connections to the WRDS services. */
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT;

    static
    {
        try
        {
            SSL_CONTEXT = ReaderUtilities.getSslContextTrustingDodSignerForWrds();
        }
        catch ( PreIngestException e )
        {
            throw new ExceptionInInitializerError( "Failed to acquire the TLS context for connecting to WRDS: "
                                                   + e.getMessage() );
        }
    }

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT );

    /** Pair declaration, which is used to chunk requests. Null if no chunking is required. */
    private final PairConfig pairConfig;

    /** A thread pool to process web requests. */
    private final ThreadPoolExecutor executor;

    /**
     * @see #of(PairConfig, SystemSettings)
     * @param systemSettings the system settings
     * @return an instance that does not performing any chunking of the time-series data
     * @throws NullPointerException if the systemSettings is null
     */

    public static WrdsAhpsReader of( SystemSettings systemSettings )
    {
        return new WrdsAhpsReader( null, systemSettings );
    }

    /**
     * @param pairConfig the pair declaration, which is used to perform chunking of a data source
     * @param systemSettings the system settings
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    public static WrdsAhpsReader of( PairConfig pairConfig, SystemSettings systemSettings )
    {
        Objects.requireNonNull( pairConfig );

        return new WrdsAhpsReader( pairConfig, systemSettings );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Chunk the requests if needed
        if ( Objects.nonNull( this.getPairConfig() ) )
        {
            LOGGER.debug( "Preparing requests for WRDS AHPS time-series that chunk the time-series data by feature and "
                          + "time range." );
            return this.read( dataSource, this.getPairConfig() );
        }

        LOGGER.debug( "Preparing a request to WRDS for AHPS time-series without any chunking of the data." );
        InputStream stream = WrdsAhpsReader.getByteStreamFromUri( dataSource.getUri() );
        return this.read( dataSource, stream );
    }

    /**
     * This implementation is equivalent to calling {@link WatermlReader#read(DataSource, InputStream)}.
     * @param dataSource the data source, required
     * @param stream the input stream, required
     * @return the stream of time-series
     * @throws NullPointerException if any input is null
     * @throws ReadException if the reading fails for any other reason
     */

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
     * @return the pair declaration, possibly null
     */

    private PairConfig getPairConfig()
    {
        return this.pairConfig;
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
     * @param pairConfig the pair declaration used for chunking
     * @throws NullPointerException if either input is null
     */

    private Stream<TimeSeriesTuple> read( DataSource dataSource, PairConfig pairConfig )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( pairConfig );

        this.validateSource( dataSource );

        // The features
        Set<String> features = ConfigHelper.getFeatureNamesForSource( pairConfig,
                                                                      dataSource.getContext(),
                                                                      dataSource.getLeftOrRightOrBaseline() );

        // Date ranges
        Set<Pair<Instant, Instant>> dateRanges;
        if ( ReaderUtilities.isWrdsObservedSource( dataSource ) )
        {
            dateRanges = ReaderUtilities.getYearRanges( pairConfig, dataSource );
        }
        else
        {
            dateRanges = WrdsAhpsReader.getSimpleRange( pairConfig, dataSource );
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
                                                                 .getValue(),
                                                       nextChunk.getRight(),
                                                       nextChunk.getLeft(),
                                                       dataSource.getContext()
                                                                 .getUrlParameter(),
                                                       ReaderUtilities.isWrdsObservedSource( dataSource ) );

                    DataSource innerSource =
                            DataSource.of( dataSource.getDisposition(),
                                           dataSource.getSource(),
                                           dataSource.getContext(),
                                           dataSource.getLinks(),
                                           nextUri,
                                           dataSource.getLeftOrRightOrBaseline() );

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
                       try ( InputStream inputStream = WrdsAhpsReader.getByteStreamFromUri( dataSource.getUri() );
                             Stream<TimeSeriesTuple> seriesStream = AHPS_READER.read( dataSource, inputStream ) )
                       {
                           return seriesStream.toList(); // Terminal
                       }
                   } );
    }

    /**
     * @param dataSource the data source
     * @throws ReadException if the source is invalid
     */

    private void validateSource( DataSource dataSource )
    {
        if ( ! ( ReaderUtilities.isWrdsAhpsSource( dataSource )
                 || ReaderUtilities.isWrdsObservedSource( dataSource ) ) )
        {
            throw new ReadException( "Expected a WRDS AHPS data source, but got: " + dataSource + "." );
        }
    }

    /**
     * Returns a byte stream from a file or web source.
     * 
     * @param uri the uri
     * @return the byte stream
     * @throws UnsupportedOperationException if the uri scheme is not one of http(s) or file
     * @throws ReadException if the stream could not be created for any other reason
     */

    private static InputStream getByteStreamFromUri( URI uri )
    {
        Objects.requireNonNull( uri );

        if ( uri.getScheme().toLowerCase().startsWith( "http" ) )
        {
            try
            {
                // Stream is closed at a higher level
                WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri );
                int httpStatus = response.getStatusCode();

                if ( httpStatus >= 400 && httpStatus < 500 )
                {
                    LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}.",
                                 httpStatus,
                                 uri );
                }

                return response.getResponse();
            }
            catch ( IOException e )
            {
                throw new ReadException( "Failed to acquire a byte stream from "
                                         + uri
                                         + ".",
                                         e );
            }
        }
        else
        {
            throw new ReadException( "Unable to read WRDS source " + uri
                                     + "because it does not use the http "
                                     + "scheme. Did you intend to use a JSON reader?" );
        }
    }

    /**
     * Returns the exact forecast range from the declaration instead of breaking it apart. Promote to 
     * {@link ReaderUtilities} if other readers require this behavior.
     * 
     * @param pairConfig the pair declaration
     * @param dataSource the data source
     * @return a simple range
     */
    private static Set<Pair<Instant, Instant>> getSimpleRange( PairConfig pairConfig,
                                                               DataSource dataSource )
    {

        Objects.requireNonNull( pairConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        // Forecast data?
        boolean isForecast = ConfigHelper.isForecast( dataSource.getContext() );

        if ( ( isForecast && Objects.isNull( pairConfig.getIssuedDates() ) ) )
        {
            throw new ReadException( "While attempting to read forecasts from the WRDS AHPS service, discovered a data "
                                     + "source with missing issued dates, which is not allowed. Please declare issued "
                                     + "dates to constrain the read to a finite amount of time-series data." );
        }
        else if ( !isForecast && Objects.isNull( pairConfig.getDates() ) )
        {
            throw new ReadException( "While attempting to read observations from the WRDS AHPS service, discovered a "
                                     + "data source with missing dates, which is not allowed. Please declare dates to "
                                     + "constrain the read to a finite amount of time-series data." );
        }

        // When dates are present, both bookends are present because this was validated on construction of the reader
        DateCondition dates = pairConfig.getDates();

        if ( isForecast )
        {
            dates = pairConfig.getIssuedDates();
        }

        String specifiedEarliest = dates.getEarliest();
        Instant earliest = Instant.parse( specifiedEarliest );

        String specifiedLatest = dates.getLatest();
        Instant latest = Instant.parse( specifiedLatest );
        Pair<Instant, Instant> range = Pair.of( earliest, latest );
        return Set.of( range );
    }

    /**
     * Gets a URI for given date range and feature.
     *
     * <p>Expecting a wrds URI like this:
     * <a href="http://redacted/api/v1/forecasts/streamflow/ahps">http://redacted/api/v1/forecasts/streamflow/ahps</a></p>
     * @param range the range of dates (from left to right)
     * @param nwsLocationId the feature for which to get data
     * @param observed true for observations, false for AHPS forecasts
     * @return a URI suitable to get the data from WRDS API
     * @throws ReadException if the URI could not be constructed
     */

    private URI getUriForChunk( URI baseUri,
                                Pair<Instant, Instant> range,
                                String nwsLocationId,
                                List<UrlParameter> additionalParameters,
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
        if ( !basePath.contains( "v1.1" ) && !basePath.endsWith( "nws_lid/" ) )
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
     * @return the key/value parameters
     */

    private Map<String, String> createWrdsAhpsUrlParameters( Pair<Instant, Instant> dateRange,
                                                             List<UrlParameter> additionalParameters,
                                                             boolean observed )
    {
        Map<String, String> urlParameters = new HashMap<>( 2 );

        // Set the proj field, allowing for a user to override it with a URL 
        // parameter, which is handled next.
        urlParameters.put( "proj", ReaderUtilities.DEFAULT_WRDS_PROJ );

        // Caller-supplied additional parameters are lower precedence, put first
        for ( UrlParameter parameter : additionalParameters )
        {
            urlParameters.put( parameter.getName(), parameter.getValue() );
        }

        String timeTag = "issuedTime";
        if ( observed )
        {
            timeTag = "validTime";
        }

        urlParameters.put( timeTag,
                           "[" + dateRange.getLeft().toString()
                                    + ","
                                    + dateRange.getRight().toString()
                                    + "]" );

        return Collections.unmodifiableMap( urlParameters );
    }

    /**
     * Hidden constructor.
     * @param pairConfig the optional pair declaration, which is used to perform chunking of a data source
     * @param systemSettings the system settings, required
     * @throws ProjectConfigException if the project declaration is invalid for this source type
     * @throws NullPointerException if the systemSettings is null
     */

    private WrdsAhpsReader( PairConfig pairConfig, SystemSettings systemSettings )
    {
        Objects.requireNonNull( systemSettings );

        if ( Objects.nonNull( pairConfig ) )
        {
            if ( Objects.isNull( pairConfig.getDates() ) && Objects.isNull( pairConfig.getIssuedDates() ) )
            {
                throw new ProjectConfigException( pairConfig,
                                                  WHEN_USING_WRDS_AS_A_SOURCE_OF_TIME_SERIES_DATA_YOU_MUST_DECLARE
                                                              + "either the dates or issuedDates." );
            }

            if ( Objects.nonNull( pairConfig.getDates() ) && ( Objects.isNull( pairConfig.getDates().getEarliest() )
                                                               || Objects.isNull( pairConfig.getDates()
                                                                                            .getLatest() ) ) )
            {
                throw new ProjectConfigException( pairConfig,
                                                  WHEN_USING_WRDS_AS_A_SOURCE_OF_TIME_SERIES_DATA_YOU_MUST_DECLARE
                                                              + "both the earliest and latest dates (e.g. "
                                                              + "<dates earliest=\"2019-08-10T14:30:00Z\" "
                                                              + "latest=\"2019-08-15T18:00:00Z\" />)." );
            }

            if ( Objects.nonNull( pairConfig.getIssuedDates() )
                 && ( Objects.isNull( pairConfig.getIssuedDates().getEarliest() )
                      || Objects.isNull( pairConfig.getIssuedDates()
                                                   .getLatest() ) ) )
            {
                throw new ProjectConfigException( pairConfig,
                                                  WHEN_USING_WRDS_AS_A_SOURCE_OF_TIME_SERIES_DATA_YOU_MUST_DECLARE
                                                              + "both the earliest and latest issued dates (e.g. "
                                                              + "<issuedDates earliest=\"2019-08-10T14:30:00Z\" "
                                                              + "latest=\"2019-08-15T18:00:00Z\" />)." );
            }

            LOGGER.debug( "When building a reader for AHPS time-series data from the WRDS, received a complete pair "
                          + "declaration, which will be used to chunk requests by feature and time range." );
        }

        this.pairConfig = pairConfig;

        ThreadFactory webClientFactory = new BasicThreadFactory.Builder().namingPattern( "WRDS AHPS Reading Thread %d" )
                                                                         .build();

        // Use a queue with as many places as client threads
        BlockingQueue<Runnable> webClientQueue =
                new ArrayBlockingQueue<>( systemSettings.getMaximumWebClientThreads() );
        this.executor = new ThreadPoolExecutor( systemSettings.getMaximumWebClientThreads(),
                                                systemSettings.getMaximumWebClientThreads(),
                                                systemSettings.poolObjectLifespan(),
                                                TimeUnit.MILLISECONDS,
                                                webClientQueue,
                                                webClientFactory );

        // Because of use of latch and queue below, rejection should not happen.
        this.executor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
    }
}
