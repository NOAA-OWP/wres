package wres.io.reading.web;

import static java.time.DayOfWeek.SUNDAY;
import static java.time.temporal.TemporalAdjusters.next;
import static java.time.temporal.TemporalAdjusters.previousOrSame;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import wres.config.ProjectConfigException;
import wres.config.generated.DatasourceType;
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
import wres.io.reading.wrds.nwm.NwmRootDocumentWithError;
import wres.io.reading.wrds.nwm.WrdsNwmJsonReader;

/**
 * Reads time-series data from the National Weather Service (NWS) Water Resources Data Service for the National Water 
 * Model (NWM). Time-series requests are chunked by feature and date range into 25-feature blocks and weekly date 
 * ranges, respectively.
 *  
 * @author James Brown
 */

public class WrdsNwmReader implements TimeSeriesReader
{
    /** The underlying format reader for JSON-formatted data from the NWM service. */
    private static final WrdsNwmJsonReader NWM_READER = WrdsNwmJsonReader.of();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReader.class );

    /** Trust manager for TLS connections to the WRDS services. */
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT;

    /** Forward slash character. */
    private static final String SLASH = "/";

    /** The number of features to include in a chunk. */
    private static final int FEATURE_CHUNK_SIZE = 25;

    /** Re-used string. */
    private static final String WHEN_USING_WRDS_AS_A_SOURCE_OF_TIME_SERIES_DATA_YOU_MUST_DECLARE =
            "When using WRDS as a source of time-series data, you must declare ";

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

    /**
     * @return an instance that does not performing any chunking of the time-series data
     */

    public static WrdsNwmReader of()
    {
        return new WrdsNwmReader( null );
    }

    /**
     * @param pairConfig the pair declaration, which is used to perform chunking of a data source
     * @return an instance
     * @throws NullPointerException if the pairConfig is null
     */

    public static WrdsNwmReader of( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig );

        return new WrdsNwmReader( pairConfig );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Chunk the requests if needed
        if ( Objects.nonNull( this.getPairConfig() ) )
        {
            LOGGER.debug( "Preparing requests for WRDS NWM time-series that chunk the time-series data by feature and "
                          + "time range." );
            return this.read( dataSource, this.getPairConfig() );
        }

        LOGGER.debug( "Preparing a request to WRDS for NWM time-series without any chunking of the data." );
        InputStream stream = WrdsNwmReader.getByteStreamFromUri( dataSource.getUri() );
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
        LOGGER.debug( "Discovered an existing stream, assumed to be from a WRDS NWM service instance. Passing through "
                      + "to an underlying WARDS NWM JSON reader." );

        return NWM_READER.read( dataSource, stream );
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

        this.validateSource( dataSource );

        // The features
        Set<String> featureSet = ConfigHelper.getFeatureNamesForSource( pairConfig,
                                                                        dataSource.getContext(),
                                                                        dataSource.getLeftOrRightOrBaseline() );

        List<String> features = Collections.unmodifiableList( new ArrayList<>( featureSet ) );

        // The chunked features
        List<List<String>> featureBlocks = ListUtils.partition( features, FEATURE_CHUNK_SIZE );

        // Date ranges
        Set<Pair<Instant, Instant>> dateRanges = WrdsNwmReader.getWeekRanges( pairConfig, dataSource );

        // Combine the features and date ranges to form the chunk boundaries
        Set<Pair<List<String>, Pair<Instant, Instant>>> chunks = new HashSet<>();
        for ( List<String> nextFeatures : featureBlocks )
        {
            for ( Pair<Instant, Instant> nextDates : dateRanges )
            {
                Pair<List<String>, Pair<Instant, Instant>> nextChunk = Pair.of( nextFeatures, nextDates );
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
                                                             Set<Pair<List<String>, Pair<Instant, Instant>>> chunks )
    {
        LOGGER.debug( "Creating a time-series supplier to supply one time-series for each of these {} chunks: {}.",
                      chunks.size(),
                      chunks );

        SortedSet<Pair<List<String>, Pair<Instant, Instant>>> mutableChunks = new TreeSet<>( chunks );

        // Create a supplier that returns a time-series once complete
        return () -> {

            // Clean up before sending the null sentinel, which terminates the stream
            // New rows to increment
            while ( !mutableChunks.isEmpty() )
            {
                Pair<List<String>, Pair<Instant, Instant>> nextChunk = mutableChunks.first();
                mutableChunks.remove( nextChunk );

                // Create the inner data source for the chunk 
                URI nextUri = this.getUriForChunk( dataSource.getSource()
                                                             .getValue(),
                                                   dataSource,
                                                   nextChunk.getRight(),
                                                   nextChunk.getLeft() );

                DataSource innerSource =
                        DataSource.of( dataSource.getDisposition(),
                                       dataSource.getSource(),
                                       dataSource.getContext(),
                                       dataSource.getLinks(),
                                       nextUri,
                                       dataSource.getLeftOrRightOrBaseline() );

                LOGGER.debug( "Created data source for chunk, {}.", innerSource );

                // Get the stream, which is closed on the terminal operation below
                InputStream inputStream = WrdsNwmReader.getByteStreamFromUri( nextUri );

                // At most, one series, by definition
                Optional<TimeSeriesTuple> timeSeries = NWM_READER.read( dataSource, inputStream )
                                                                 .findFirst(); // Terminal

                // Return a time-series if present
                if ( timeSeries.isPresent() )
                {
                    return timeSeries.get();
                }

                LOGGER.debug( "Skipping chunk {} because no time-series were returned from WRDS.", nextChunk );
            }

            // Null sentinel to close stream
            return null;
        };
    }

    /**
     * @param dataSource the data source
     * @throws ReadException if the source is invalid
     */

    private void validateSource( DataSource dataSource )
    {
        if ( ! ( ReaderUtilities.isWrdsNwmSource( dataSource ) ) )
        {
            throw new ReadException( "Expected a WRDS NWM data source, but got: " + dataSource + "." );
        }
    }

    /**
     * Break up dates into weeks starting at T00Z Sunday and ending T00Z the next Sunday.
     *
     * <p>The purpose of chunking by weeks is re-use between evaluations. Suppose evaluation A evaluates forecasts 
     * issued December 12 through December 28. Then evaluation B evaluates forecasts issued December 13 through 
     * December 29. Rather than each evaluation ingesting the data every time the dates change, if we chunk by week, we 
     * can avoid the re-ingest of data from say, December 16 through December 22, and if we extend the chunk to each 
     * Sunday, there will be three sources, none re-ingested.
     *
     * <p>Issued dates must be specified when using an API source to avoid ambiguities and to avoid infinite data 
     * requests.
     *
     * TODO: promote this to {@link ReaderUtilities} when required by more readers.
     *
     * @param pairConfig the evaluation project pair declaration, required
     * @param dataSource the data source, required
     * @return a set of week ranges
     */

    private static Set<Pair<Instant, Instant>> getWeekRanges( PairConfig pairConfig,
                                                              DataSource dataSource )
    {
        Objects.requireNonNull( pairConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        boolean isForecast = ConfigHelper.isForecast( dataSource.getContext() );

        DateCondition dates = pairConfig.getDates();

        if ( isForecast )
        {
            dates = pairConfig.getIssuedDates();
        }

        SortedSet<Pair<Instant, Instant>> weekRanges = new TreeSet<>();

        OffsetDateTime earliest;
        String specifiedEarliest = dates.getEarliest();

        OffsetDateTime latest;
        String specifiedLatest = dates.getLatest();

        earliest = OffsetDateTime.parse( specifiedEarliest )
                                 .with( previousOrSame( SUNDAY ) )
                                 .withHour( 0 )
                                 .withMinute( 0 )
                                 .withSecond( 0 )
                                 .withNano( 0 );

        LOGGER.debug( "Given {} calculated {} for earliest.",
                      specifiedEarliest,
                      earliest );

        // Intentionally keep this raw, un-Sunday-ified.
        latest = OffsetDateTime.parse( specifiedLatest );

        LOGGER.debug( "Given {} parsed {} for latest.",
                      specifiedLatest,
                      latest );

        OffsetDateTime left = earliest;
        OffsetDateTime right = left.with( next( SUNDAY ) );

        OffsetDateTime nowDate = OffsetDateTime.now();

        while ( left.isBefore( latest ) )
        {
            // Because we chunk a week at a time, and because these will not
            // be retrieved again if already present, we need to ensure the
            // right hand date does not exceed "now".
            if ( right.isAfter( nowDate ) )
            {
                if ( latest.isAfter( nowDate ) )
                {
                    right = nowDate;
                }
                else
                {
                    right = latest;
                }
            }

            Pair<Instant, Instant> range = Pair.of( left.toInstant(), right.toInstant() );
            LOGGER.debug( "Created range {}", range );
            weekRanges.add( range );
            left = left.with( next( SUNDAY ) );
            right = right.with( next( SUNDAY ) );
        }

        LOGGER.debug( "Calculated ranges {}", weekRanges );

        return Collections.unmodifiableSet( weekRanges );
    }

    /**
     * Gets a URI for given date range and feature.
     *
     * <p>Expecting a wrds URI like this:
     * http://redacted/api/v1/forecasts/streamflow/ahps</p>
     * @param baseUri the base URI
     * @param dataSource the data source
     * @param range the range of dates (from left to right)
     * @param featureNames the feature names for which to get data
     * @return a URI suitable to get the data from WRDS NWM API
     * @throws ReadException if the URI could not be constructed
     */

    private URI getUriForChunk( URI baseUri,
                                DataSource dataSource,
                                Pair<Instant, Instant> range,
                                List<String> featureNames )
    {
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( range );
        Objects.requireNonNull( featureNames );

        String variableName = dataSource.getVariable()
                                        .getValue();

        String basePath = baseUri.getPath();

        // Tolerate either a slash at end or not.
        if ( !basePath.endsWith( SLASH ) )
        {
            basePath = basePath + SLASH;
        }

        boolean isEnsemble = dataSource.getContext()
                                       .getType() == DatasourceType.ENSEMBLE_FORECASTS;

        Map<String, String> wrdsParameters = this.createWrdsNwmUrlParameters( range,
                                                                              isEnsemble,
                                                                              dataSource.getContext()
                                                                                        .getUrlParameter() );
        StringJoiner joiner = new StringJoiner( "," );

        for ( String featureName : featureNames )
        {
            joiner.add( featureName );
        }

        String featureNamesCsv = joiner.toString();

        LOGGER.debug( "Adding these features to the URI: {}.", featureNamesCsv );

        String pathWithLocation = basePath + variableName
                                  + "/nwm_feature_id/"
                                  + featureNamesCsv
                                  + SLASH;

        if ( pathWithLocation.length() > 1960 )
        {
            LOGGER.warn( "Built an unexpectedly long path: {}",
                         pathWithLocation );
        }

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
     * Specific to WRDS NWM API, get date range url parameters
     * @param range the date range to set parameters for
     * @return the key/value parameters
     */

    private Map<String, String> createWrdsNwmUrlParameters( Pair<Instant, Instant> range,
                                                            boolean isEnsemble,
                                                            List<UrlParameter> additionalParameters )
    {
        Map<String, String> urlParameters = new HashMap<>( 3 );

        // Start with a WRES guess here, but allow this one to be overridden by
        // caller-supplied additional parameters. See #76880
        if ( isEnsemble )
        {
            urlParameters.put( "forecast_type", "ensemble" );
        }
        else
        {
            urlParameters.put( "forecast_type", "deterministic" );
        }

        // Set the default WRDS proj, but allow a user to override it
        // through URL parameter processed below.
        urlParameters.put( "proj", ReaderUtilities.DEFAULT_WRDS_PROJ );

        // Caller-supplied additional parameters are lower precedence, put first
        // This will override the parameter added above.
        for ( UrlParameter parameter : additionalParameters )
        {
            urlParameters.put( parameter.getName(), parameter.getValue() );
        }

        String leftWrdsFormattedDate = WrdsNwmReader.iso8601TruncatedToHourFromInstant( range.getLeft() );
        String rightWrdsFormattedDate = WrdsNwmReader.iso8601TruncatedToHourFromInstant( range.getRight() );
        urlParameters.put( "reference_time",
                           "(" + leftWrdsFormattedDate
                                             + ","
                                             + rightWrdsFormattedDate
                                             + "]" );
        urlParameters.put( "validTime", "all" );

        return Collections.unmodifiableMap( urlParameters );
    }

    /**
     * The WRDS NWM API uses a concise ISO-8601 format rather than RFC3339 instant.
     * @param instant the instance
     * @return the ISO-8601 instant string
     */
    private static String iso8601TruncatedToHourFromInstant( Instant instant )
    {
        String dateFormat = "uuuuMMdd'T'HHX";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( dateFormat )
                                                       .withZone( ZoneId.of( "UTC" ) );
        return formatter.format( instant );
    }

    /**
     * Returns a byte stream from a URI.
     * 
     * @param uri the URI
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
                // Stream us closed at a higher level
                WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri );
                int httpStatus = response.getStatusCode();

                // Read an error if possible
                if ( httpStatus >= 400 && httpStatus < 500 )
                {
                    String possibleError = WrdsNwmReader.tryToReadErrorMessage( response.getResponse() );

                    if ( Objects.nonNull( possibleError ) )
                    {
                        LOGGER.warn( "Found this WRDS error message from URI {}: {}",
                                     uri,
                                     possibleError );
                    }

                    return null;
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
     * Attempt to read an error message from the WRDS NWM service for a document like this:
     * {
     *   "error": "API Currently only supports querying by the following: ('nwm_feature_id', 'nws_lid', ... )"
     * }
     *
     * If anything goes wrong, returns null.
     *
     * @param inputStream the stream containing a potential error message
     * @return the value from the above map, null if not found.
     */

    private static String tryToReadErrorMessage( InputStream inputStream )
    {
        try
        {
            ObjectMapper mapper = new ObjectMapper();
            NwmRootDocumentWithError document = mapper.readValue( inputStream,
                                                                  NwmRootDocumentWithError.class );
            Map<String, String> messages = document.getMessages();

            if ( Objects.nonNull( messages ) )
            {
                return messages.get( "error" );
            }
        }
        catch ( IOException ioe )
        {
            LOGGER.debug( "Failed to parse an error response body.", ioe );
        }

        return null;
    }

    /**
     * Hidden constructor.
     * @param pairConfig the optional pair declaration, which is used to perform chunking of a data source
     * @throws ProjectConfigException if the project declaration is invalid for this source type
     */

    private WrdsNwmReader( PairConfig pairConfig )
    {
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

            LOGGER.debug( "When building a reader for NWM time-series data from the WRDS, received a complete pair "
                          + "declaration, which will be used to chunk requests by feature and time range." );
        }
    
        this.pairConfig = pairConfig;
    }

}
