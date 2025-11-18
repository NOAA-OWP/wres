package wres.reading.nwis.iv;

import java.io.InputStream;
import java.net.URI;
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
import java.util.StringJoiner;
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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.DeclarationException;
import wres.config.DeclarationUtilities;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.Variable;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.reading.nwis.iv.response.ResponseReader;
import wres.statistics.generated.GeometryTuple;
import wres.system.SystemSettings;

/**
 * Reads time-series data from the National Water Information System (NWIS) Instantaneous Values (IV) web service. This 
 * service provides access to observed time-series whose event values always have an instantaneous time scale. The
 * service and its API is described here:
 *
 * <p><a href="https://waterservices.usgs.gov/rest/IV-Service.html">USGS NWIS IV Web Service</a> 
 *
 * <p>The above link was last accessed: 20220804T11:00Z.
 *
 * <p>Implementation notes:
 *
 * <p>This implementation reads time-series data in WaterML format only. The NWIS IV Service does support other formats, 
 * but WaterML is the default. Time-series are chunked by feature and year ranges. The underlying format reader is a
 * {@link ResponseReader}.
 *
 * @author James Brown
 * @author Jesse Bickel
 * @author Christopher Tubbs
 */

public class NwisIvReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwisIvReader.class );

    /** The underlying format reader. */
    private static final ResponseReader WATERML_READER = ResponseReader.of();

    /** Message string. */
    private static final String USGS_NWIS = "USGS NWIS";

    /** The HTTP response codes considered to represent no data. */
    private static final IntPredicate NO_DATA_PREDICATE = c -> c == 404;

    /** The HTTP response codes considered to represent an error to be thrown. */
    private static final IntPredicate ERROR_RESPONSE_PREDICATE = c -> !( c >= 200 && c < 300 );

    /** Pair declaration, which is used to chunk requests. Null if no chunking is required. */
    private final EvaluationDeclaration declaration;

    /** A thread pool to process web requests. */
    private final ThreadPoolExecutor executor;

    /**
     * @see #of(EvaluationDeclaration, SystemSettings)
     * @param systemSettings the system settings
     * @return an instance that does not perform any chunking of the time-series data
     * @throws NullPointerException if the systemSettings is null
     */

    public static NwisIvReader of( SystemSettings systemSettings )
    {
        return new NwisIvReader( null, systemSettings );
    }

    /**
     * @param declaration the declaration, which is used to perform chunking of a data source
     * @param systemSettings the system settings
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    public static NwisIvReader of( EvaluationDeclaration declaration, SystemSettings systemSettings )
    {
        Objects.requireNonNull( declaration );

        return new NwisIvReader( declaration, systemSettings );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Chunk the requests if needed
        if ( Objects.nonNull( this.getDeclaration() ) )
        {
            LOGGER.debug( "Preparing requests for the USGS NWIS that chunk the time-series data by feature and "
                          + "time range." );
            return this.read( dataSource, this.getDeclaration() );
        }

        LOGGER.debug( "Preparing a request to NWIS for USGS time-series without any chunking of the data." );

        InputStream stream = ReaderUtilities.getByteStreamFromWebSource( dataSource.getUri(),
                                                                         NO_DATA_PREDICATE,
                                                                         ERROR_RESPONSE_PREDICATE,
                                                                         null,
                                                                         null );

        return this.read( dataSource, stream );
    }

    /**
     * This implementation is equivalent to calling {@link ResponseReader#read(DataSource, InputStream)}.
     * @param dataSource the data source, required
     * @param stream the input stream, required
     * @return the stream of time-series
     * @throws NullPointerException if the dataSource is null
     * @throws ReadException if the reading fails for any other reason
     */

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream stream )
    {
        LOGGER.debug( "Discovered an existing stream, assumed to be from a USGS NWIS IV service instance. Passing "
                      + "through to an underlying WaterML reader." );

        return WATERML_READER.read( dataSource, stream );
    }

    /**
     * @return the pair declaration, possibly null
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

        // The features
        Set<GeometryTuple> geometries = DeclarationUtilities.getFeatures( declaration );

        Set<String> features = ReaderUtilities.getFeatureNamesFor( geometries, dataSource );

        // Date ranges
        Set<Pair<Instant, Instant>> dateRanges = ReaderUtilities.getYearRanges( declaration, dataSource );

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
                                                       dataSource,
                                                       nextChunk.getRight(),
                                                       nextChunk.getLeft() );

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
                                                                              USGS_NWIS );

                cachedSeries.addAll( result );

                // Still some chunks to request or results to return?
                proceed.set( !mutableChunks.isEmpty()
                             || !results.isEmpty()
                             || cachedSeries.size() > 1 );

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
                       try ( InputStream s = ReaderUtilities.getByteStreamFromWebSource( dataSource.getUri(),
                                                                                         NO_DATA_PREDICATE,
                                                                                         ERROR_RESPONSE_PREDICATE,
                                                                                         null,
                                                                                         null ) )
                       {
                           if ( Objects.nonNull( s ) )
                           {
                               return WATERML_READER.read( dataSource, s )
                                                    .toList(); // Terminal
                           }

                           return List.of();
                       }
                   } );
    }

    /**
     * Get a URI for a given date range and feature.
     *
     * <p>Expecting a USGS URI like this:
     * <a href="https://nwis.waterservices.usgs.gov/nwis/iv/">https://nwis.waterservices.usgs.gov/nwis/iv/</a></p>
     *
     * @param baseUri the base uri associated with this source
     * @param dataSource the data source for which to create a URI
     * @param range the range of dates (from left to right)
     * @param featureNames the features to include in the request URI
     * @return a URI suitable to get the data from WRDS API
     */

    private URI getUriForChunk( URI baseUri,
                                DataSource dataSource,
                                Pair<Instant, Instant> range,
                                String... featureNames )
    {
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( range );
        Objects.requireNonNull( featureNames );
        Objects.requireNonNull( range.getLeft() );
        Objects.requireNonNull( range.getRight() );

        if ( !baseUri.toString()
                     .toLowerCase()
                     .contains( "nwis/iv" )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Expected a URI like 'https://nwis.waterservices.usgs.gov/nwis/iv' but got {}.",
                         baseUri );
        }

        Map<String, String> urlParameters = this.getUrlParameters( range,
                                                                   featureNames,
                                                                   dataSource );
        return ReaderUtilities.getUriWithParameters( baseUri,
                                                     urlParameters );
    }

    /**
     * Specific to USGS NWIS API, get date range url parameters
     * @param range the date range to set parameters for
     * @param siteCodes The USGS site codes desired.
     * @param dataSource The data source from which this request came.
     * @return the key/value parameters
     * @throws NullPointerException When arg or value enclosed inside arg is null
     */

    private Map<String, String> getUrlParameters( Pair<Instant, Instant> range,
                                                  String[] siteCodes,
                                                  DataSource dataSource )
    {
        LOGGER.trace( "Called getUrlParameters with {}, {}, {}",
                      range,
                      siteCodes,
                      dataSource );

        Objects.requireNonNull( range );
        Objects.requireNonNull( siteCodes );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( range.getLeft() );
        Objects.requireNonNull( range.getRight() );
        Objects.requireNonNull( dataSource.getVariable() );
        Objects.requireNonNull( dataSource.getVariable()
                                          .name() );

        StringJoiner siteJoiner = new StringJoiner( "," );

        for ( String site : siteCodes )
        {
            siteJoiner.add( site );
        }

        // The start datetime needs to be one second later than the next
        // week-range's end datetime or we end up with duplicates.
        // This follows the "pools are inclusive/exclusive" convention of WRES.
        // For some reason, 1 to 999 milliseconds are not enough.
        Instant startDateTime = range.getLeft()
                                     .plusSeconds( 1 );
        Map<String, String> urlParameters = new HashMap<>( dataSource.getSource()
                                                                     .parameters() );

        String parameterCodes = this.getParameterCodes( dataSource.getVariable() );
        urlParameters.put( "format", "json" );
        urlParameters.put( "parameterCd", parameterCodes );
        urlParameters.put( "startDT", startDateTime.toString() );
        urlParameters.put( "endDT", range.getRight().toString() );
        urlParameters.put( "sites", siteJoiner.toString() );

        return Collections.unmodifiableMap( urlParameters );
    }

    /**
     * @param variable the variable
     * @return the parameter codes
     */
    private String getParameterCodes( Variable variable )
    {
        StringJoiner start = new StringJoiner( "," ).add( variable.name() );

        if ( !variable.aliases()
                      .isEmpty() )
        {
            variable.aliases()
                    .forEach( start::add );
            LOGGER.debug( "Added the following aliased variable names for variable {}: {}",
                          variable.name(),
                          variable.aliases() );
        }

        return start.toString();
    }

    /**
     * Hidden constructor.
     * @param declaration the optional declaration, which is used to perform chunking of a data source
     * @param systemSettings the system settings
     * @throws DeclarationException if the project declaration is invalid for this source type
     * @throws NullPointerException if the systemSettings is null
     */

    private NwisIvReader( EvaluationDeclaration declaration, SystemSettings systemSettings )
    {
        Objects.requireNonNull( systemSettings );

        this.declaration = declaration;

        ThreadFactory webClientFactory = BasicThreadFactory.builder()
                                                           .namingPattern( "USGS NWIS Reading Thread %d" )
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
