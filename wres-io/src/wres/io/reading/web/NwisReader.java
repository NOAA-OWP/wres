package wres.io.reading.web;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.PairConfig;
import wres.config.generated.UrlParameter;
import wres.datamodel.time.TimeSeriesTuple;
import wres.io.config.ConfigHelper;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.waterml.WatermlReader;

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
 * but WaterML is the default. Time-series are chunked by feature and year ranges.
 * 
 * @author James Brown
 * @author Jesse Bickel
 * @author Christopher Tubbs
 */

public class NwisReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwisReader.class );

    /** The underlying format reader. */
    private static final WatermlReader WATERML_READER = WatermlReader.of();

    /** A web client to help with reading data from the web. */
    private static final WebClient WEB_CLIENT = new WebClient();

    /** Pair declaration, which is used to chunk requests. Null if no chunking is required. */
    private final PairConfig pairConfig;

    /**
     * @see #of(PairConfig)
     * @return an instance that does not performing any chunking of the time-series data
     */

    public static NwisReader of()
    {
        return new NwisReader( null );
    }

    /**
     * @param pairConfig the pair declaration, which is used to perform chunking of a data source
     * @return an instance
     * @throws NullPointerException if the pairConfig is null
     */

    public static NwisReader of( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig );

        return new NwisReader( pairConfig );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Chunk the requests if needed
        if ( Objects.nonNull( this.getPairConfig() ) )
        {
            LOGGER.debug( "Preparing requests for the USGS NWIS that chunk the time-series data by feature and "
                          + "time range." );
            return this.read( dataSource, this.getPairConfig() );
        }

        LOGGER.debug( "Preparing a request to NWIS for USGS time-series without any chunking of the data." );
        InputStream stream = NwisReader.getByteStreamFromUri( dataSource.getUri() );

        return this.read( dataSource, stream );
    }

    /**
     * This implementation is equivalent to calling {@link WatermlReader#read(DataSource, InputStream)}.
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

    private PairConfig getPairConfig()
    {
        return this.pairConfig;
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

        // The features
        Set<String> features = ConfigHelper.getFeatureNamesForSource( pairConfig,
                                                                      dataSource.getContext(),
                                                                      dataSource.getLeftOrRightOrBaseline() );

        // Date ranges
        Set<Pair<Instant, Instant>> dateRanges = ReaderUtilities.getYearRanges( pairConfig, dataSource );

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
                     .onClose( () -> LOGGER.debug( "Detected a stream close event. Proceeding nominally as there are "
                                                   + "no resources to close." ) );
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

        // Create a supplier that returns a time-series once complete
        return () -> {

            // Clean up before sending the null sentinel, which terminates the stream
            // New rows to increment
            while ( !mutableChunks.isEmpty() )
            {
                Pair<String, Pair<Instant, Instant>> nextChunk = mutableChunks.first();
                mutableChunks.remove( nextChunk );

                // Create the inner data source for the chunk 
                URI nextUri = this.getUriForChunk( dataSource.getUri(),
                                                   dataSource,
                                                   nextChunk.getRight(),
                                                   nextChunk.getLeft() );

                DataSource innerSource =
                        DataSource.of( dataSource.getDisposition(),
                                       dataSource.getSource(),
                                       dataSource.getContext(),
                                       // Pass through the links because we
                                       // trust the SourceLoader to have
                                       // deduplicated this source if it was
                                       // repeated in other contexts.
                                       dataSource.getLinks(),
                                       nextUri,
                                       dataSource.getLeftOrRightOrBaseline() );

                LOGGER.debug( "Created data source for chunk, {}.", innerSource );

                // Get the stream, which is closed on the terminal operation below
                InputStream inputStream = NwisReader.getByteStreamFromUri( nextUri );

                // At most, one series, by definition
                Optional<TimeSeriesTuple> timeSeries = WATERML_READER.read( dataSource, inputStream )
                                                                     .findFirst(); // Terminal

                // Return a time-series if present
                if ( timeSeries.isPresent() )
                {
                    return timeSeries.get();
                }

                LOGGER.debug( "Skipping chunk {} because no time-series were returned from USGS NWIS.", nextChunk );
            }

            // Null sentinel to close stream
            return null;
        };
    }

    /**
     * Get a URI for a given date range and feature.
     *
     * <p>Expecting a USGS URI like this:
     * https://nwis.waterservices.usgs.gov/nwis/iv/</p>
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

        if ( !baseUri.getHost()
                     .toLowerCase()
                     .contains( "usgs.gov" )
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
        Objects.requireNonNull( dataSource.getVariable().getValue() );

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
        Map<String, String> urlParameters = new HashMap<>( 5 );

        // Caller-supplied additional parameters are lower precedence, put first
        for ( UrlParameter parameter : dataSource.getContext()
                                                 .getUrlParameter() )
        {
            urlParameters.put( parameter.getName(), parameter.getValue() );
        }

        urlParameters.put( "format", "json" );
        urlParameters.put( "parameterCd",
                           dataSource.getVariable()
                                     .getValue() );
        urlParameters.put( "startDT", startDateTime.toString() );
        urlParameters.put( "endDT", range.getRight().toString() );
        urlParameters.put( "sites", siteJoiner.toString() );

        return Collections.unmodifiableMap( urlParameters );
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

        try
        {
            if ( uri.getScheme().toLowerCase().startsWith( "http" ) )
            {
                // Stream is closed at a higher level
                WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri );
                int httpStatus = response.getStatusCode();

                if ( httpStatus == 404 )
                {
                    LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                                 httpStatus,
                                 uri );

                    return null;
                }
                else if ( ! ( httpStatus >= 200 && httpStatus < 300 ) )
                {
                    throw new ReadException( "Failed to read data from '"
                                             + uri
                                             +
                                             "' due to HTTP status code "
                                             + httpStatus );
                }

                return response.getResponse();
            }
            else
            {
                throw new ReadException( "Cannot read USGS NWIS time-series data from "
                                         + uri
                                         + ". Did you intend to use a WaterML reader?" );
            }
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to acquire a byte stream from "
                                     + uri
                                     + ".",
                                     e );
        }
    }

    /**
     * Hidden constructor.
     * @param pairConfig the optional pair declaration, which is used to perform chunking of a data source
     * @throws ProjectConfigException if the project declaration is invalid for this source type
     */

    private NwisReader( PairConfig pairConfig )
    {
        this.pairConfig = pairConfig;
        if ( Objects.nonNull( this.pairConfig ) )
        {
            if ( Objects.isNull( pairConfig.getDates() ) || Objects.isNull( pairConfig.getDates().getEarliest() )
                 || Objects.isNull( pairConfig.getDates().getLatest() ) )
            {
                throw new ProjectConfigException( pairConfig,
                                                  "One must specify dates with both "
                                                              + "earliest and latest (e.g. "
                                                              + "<dates earliest=\"2019-08-10T14:30:00Z\" "
                                                              + "latest=\"2019-08-15T18:00:00Z\" />) "
                                                              + "when using a web API as a source for observations." );
            }

            LOGGER.debug( "When building a reader for time-series data from the USGS NWIS service, received a complete "
                          + "pair declaration, which will be used to chunk requests by feature and time range." );
        }
    }

}
