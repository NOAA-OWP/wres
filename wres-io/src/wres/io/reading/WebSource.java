package wres.io.reading;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.DayOfWeek.SUNDAY;
import static java.time.temporal.TemporalAdjusters.next;
import static java.time.temporal.TemporalAdjusters.previousOrSame;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.IngestSaver;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * Takes a single web source and splits it into week-long chunks, creates an
 * ingest task for each chunk, GETs chunks of data several at a time, ingests
 * them, and returns the results.
 */
class WebSource implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebSource.class );

    private static final String DATES_ERROR_MESSAGE =
            "One must specify issued dates with both earliest and latest (e.g. "
            + "<issuedDates earliest=\"2018-12-28T15:42:00Z\" "
            + "latest=\"2019-01-01T00:00:00Z\" />) when using a web API as a "
            + "source for forecasts. One must specify dates with both "
            + "earliest and latest (e.g. "
            + "<dates earliest=\"2019-08-10T14:30:00Z\" "
            + "latest=\"2019-08-15T18:00:00Z\" />) "
            + "when using a web API as a source for observations.";

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final URI baseUri;
    private final OffsetDateTime now;

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingResults;

    static WebSource of( ProjectConfig projectConfig,
                         DataSource dataSource,
                         DatabaseLockManager lockManager )
    {
        return new WebSource( projectConfig,
                              dataSource,
                              lockManager,
                              OffsetDateTime.now() );
    }

    WebSource( ProjectConfig projectConfig,
               DataSource dataSource,
               DatabaseLockManager lockManager,
               OffsetDateTime now )
    {
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.baseUri = dataSource.getSource()
                                 .getValue();
        this.lockManager = lockManager;

        if ( this.baseUri.getScheme() == null
             || !this.baseUri.getScheme().startsWith( "http" ) )
        {
            throw new IllegalArgumentException( "URI " + this.baseUri.toString()
                                                + " does not appear to be a web source." );
        }

        ThreadFactory webClientFactory = new BasicThreadFactory.Builder()
                .namingPattern( "WebSource Ingest" )
                .build();

        int concurrentCount = SystemSettings.getMaximumWebClientThreads();

        // In the case of the WRDS NWM service, we have been asked to take it
        // easy on the service by not doing concurrent requests.
        InterfaceShortHand interfaceShortHand = dataSource.getSource()
                                                          .getInterface();
        if ( Objects.nonNull( interfaceShortHand )
             && interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM ) )
        {
            concurrentCount = 1;
        }

        // Because we use a latch and queue below, no need to make this queue
        // any larger than that queue and latch.
        BlockingQueue<Runnable> webClientQueue = new ArrayBlockingQueue<>( concurrentCount );
        this.ingestSaverExecutor = new ThreadPoolExecutor( concurrentCount,
                                                           concurrentCount,
                                                           SystemSettings.poolObjectLifespan(),
                                                           TimeUnit.MILLISECONDS,
                                                           webClientQueue,
                                                           webClientFactory );
        // Because of use of latch and queue below, rejection should not happen.
        this.ingestSaverExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );

        // The size of this queue is equal to the setting for simultaneous web
        // client threads so that we can 1. get quick feedback on exception
        // (which requires a small queue) and 2. allow some requests to go out
        // prior to get-one-response-per-submission-of-one-ingest-task
        this.ingests = new ArrayBlockingQueue<>( concurrentCount );
        // The size of this latch is for reason (2) above
        this.startGettingResults = new CountDownLatch( concurrentCount );
        this.now = now;
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private DataSourceConfig getDataSourceConfig()
    {
        return this.dataSource.getContext();
    }

    private DataSourceConfig.Source getSourceConfig()
    {
        return this.dataSource.getSource();
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    private DataSource getDataSource()
    {
        return this.dataSource;
    }

    private URI getBaseUri()
    {
        return this.baseUri;
    }

    private ThreadPoolExecutor getIngestSaverExecutor()
    {
        return this.ingestSaverExecutor;
    }

    private OffsetDateTime getNow()
    {
        return this.now;
    }

    @Override
    public List<IngestResult> call() throws IOException
    {
        List<IngestResult> ingestResults = new ArrayList<>();

        Set<FeatureDetails> features;

        try
        {
            features = Features.getAllDetails( this.getProjectConfig() );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to get features/locations.", se );
        }

        Set<Pair<Instant,Instant>> weekRanges = createWeekRanges( this.getProjectConfig(),
                                                                  this.getDataSource(),
                                                                  this.getNow() );

        Set<URI> alreadySubmittedUris = new HashSet<>();
        SortedSet<String> allKnownUsgsGageIds = this.getAllKnownUsgsGageIds();

        LOGGER.debug( "Found all these gage ids: {}", allKnownUsgsGageIds );

        try
        {
            for ( FeatureDetails featureDetails : features )
            {
                for ( Pair<Instant, Instant> range : weekRanges )
                {
                    boolean shouldCreateUri = shouldCreateUri( this.getBaseUri(),
                                                               range,
                                                               featureDetails );

                    if ( !shouldCreateUri )
                    {
                        LOGGER.warn( "Unable or unwilling to create a URI for feature {}, skipping it.",
                                     featureDetails );
                        continue;
                    }

                    URI uri = createUri( this.getBaseUri(),
                                         this.getDataSource(),
                                         range,
                                         featureDetails,
                                         allKnownUsgsGageIds );

                    if ( alreadySubmittedUris.contains( uri ) )
                    {
                        LOGGER.debug( "Already submitted uri {}, not re-submitting it.",
                                      uri );
                        continue;
                    }

                    DataSource dataSource =
                            DataSource.of( this.getSourceConfig(),
                                           this.getDataSourceConfig(),
                                           // Pass through the links because we
                                           // trust the SourceLoader to have
                                           // deduplicated this source if it was
                                           // repeated in other contexts.
                                           this.getDataSource()
                                               .getLinks(),
                                           uri );
                    LOGGER.debug( "Created datasource {}", dataSource);

                    IngestSaver ingestSaver =
                            IngestSaver.createTask()
                                       .withDataSource( dataSource )
                                       .withProject( this.getProjectConfig() )
                                       .withoutHash()
                                       .withLockManager( this.getLockManager() )
                                       .build();

                    Future<List<IngestResult>> future =
                            this.getIngestSaverExecutor()
                                .submit( ingestSaver );
                    this.ingests.add( future );

                    alreadySubmittedUris.add( uri );

                    // Check that all is well with previously submitted tasks, but
                    // only after a handful have been submitted. This means that
                    // an exception should propagate relatively shortly after it
                    // occurs with the ingest task. It also means after creation
                    // of a handful of tasks, we only create one after a
                    // previously created one has been completed, fifo/lockstep.
                    this.startGettingResults.countDown();

                    if ( this.startGettingResults.getCount() <= 0 )
                    {
                        List<IngestResult> ingested = this.ingests.take()
                                                                  .get();
                        ingestResults.addAll( ingested );
                    }
                }
            }

            // Finish getting the remainder of ingest results.
            for ( Future<List<IngestResult>> ingested : this.ingests )
            {
                ingestResults.addAll( ingested.get() );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while getting web ingest results.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            throw new IngestException( "Failed to get web ingest results.", ee );
        }

        return Collections.unmodifiableList( ingestResults );
    }


    /**
     * Break up dates into weeks starting at T00Z Sunday and ending T00Z the
     * next sunday. Ingest a week based on issued dates, let the retrieval
     * filter further by valid dates. We are here going to get a superset of
     * what is needed when the user specified issued dates.
     *
     * <p>The purpose of chunking by weeks is re-use between evaluations.
     * Suppose evaluation A evaluates forecasts issued December 12 through
     * December 28. Then evaluation B evaluates forecasts issued December 13
     * through December 29. Rather than each evaluation ingesting the data every
     * time the dates change, if we chunk by week, we can avoid the re-ingest of
     * data from say, December 16 through December 22, and if we extend the
     * chunk to each Sunday, there will be three sources, none re-ingested.</p>
     *
     * <p>Issued dates must be specified when using an API source to avoid
     * ambiguities and to avoid infinite data requests.</p>
     *
     * <p>Not specific to a particular API.</p>
     * @param config the evaluation project configuration, non-null, pair non-null
     * @return a set of week ranges
     * @throws ProjectConfigException when config is insufficient to make ranges
     */

    private Set<Pair<Instant,Instant>> createWeekRanges( ProjectConfig config,
                                                         DataSource dataSource,
                                                         OffsetDateTime nowDate )
    {
        Objects.requireNonNull( config );
        Objects.requireNonNull( config.getPair() );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        boolean isForecast = ConfigHelper.isForecast( dataSource.getContext() );

        if ( ( isForecast && config.getPair().getIssuedDates() == null )
             || ( !isForecast && config.getPair().getDates() == null ) )
        {
            throw new ProjectConfigException( config.getPair(),
                                              DATES_ERROR_MESSAGE );
        }

        DateCondition dates = config.getPair()
                                    .getDates();

        if ( isForecast )
        {
            dates = config.getPair()
                          .getIssuedDates();
        }

        if ( dates.getEarliest() == null
             || dates.getLatest() == null )
        {
            throw new ProjectConfigException( dates,
                                              DATES_ERROR_MESSAGE );
        }

        Set<Pair<Instant,Instant>> weekRanges = new HashSet<>();

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
                      specifiedEarliest, earliest );

        // Intentionally keep this raw, un-sunday-ified.
        latest = OffsetDateTime.parse( specifiedLatest );

        LOGGER.debug( "Given {} parsed {} for latest.",
                      specifiedLatest, latest );

        OffsetDateTime left = earliest;
        OffsetDateTime right = left.with( next( SUNDAY ) );

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

            Pair<Instant,Instant> range = Pair.of( left.toInstant(), right.toInstant() );
            LOGGER.debug( "Created range {}", range );
            weekRanges.add( range );
            left = left.with( next( SUNDAY ) );
            right = right.with( next( SUNDAY ) );
        }

        LOGGER.debug( "Calculated ranges {}", weekRanges );

        return Collections.unmodifiableSet( weekRanges );
    }


    /**
     * Tests if createUri call should succeed. Kind of awkward, but better than
     * having createUri() return null, allows createUri to guarantee a URI.
     * @param baseUri
     * @param range
     * @param featureDetails
     * @return
     */
    private boolean shouldCreateUri( URI baseUri,
                                     Pair<Instant,Instant> range,
                                     FeatureDetails featureDetails )
    {
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( range );
        Objects.requireNonNull( featureDetails );
        InterfaceShortHand interfaceShortHand = this.getDataSource()
                                                    .getSource()
                                                    .getInterface();

        if ( ( baseUri.getHost()
                      .toLowerCase()
                      .contains( "usgs.gov" )
               || ( Objects.nonNull( interfaceShortHand )
                    && interfaceShortHand.equals( InterfaceShortHand.USGS_NWIS ) )
             )
            && ( Objects.isNull( featureDetails.getGageID() )
                 || featureDetails.getGageID().isBlank() ) )
        {
            LOGGER.debug( "Avoiding null or blank usgs site code for feature {} when looking for USGS data: {}",
                          featureDetails, this );
            return false;
        }
        else if ( Objects.nonNull( interfaceShortHand )
                  && interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM )
                  && ( Objects.isNull( featureDetails.getComid() ) ) )
        {
            LOGGER.debug( "Avoiding null NWM feature id for feature {} when looking for WRDS NWM data: {}",
                          featureDetails, this );
            return false;
        }

        return true;
    }

    private URI createUri( URI baseUri,
                           DataSource dataSource,
                           Pair<Instant,Instant> range,
                           FeatureDetails featureDetails,
                           SortedSet<String> allKnownUsgsGageIds )
    {
        if ( baseUri.getHost()
                    .toLowerCase()
                    .contains( "usgs.gov" )
             || baseUri.getPath()
                       .toLowerCase()
                       .contains( "nwis" ) )
        {
            return this.createUsgsNwisUri( baseUri, range, featureDetails, allKnownUsgsGageIds );
        }
        else if ( baseUri.getPath()
                         .toLowerCase()
                         .endsWith( "ahps" ) ||
                  baseUri.getPath()
                         .toLowerCase()
                         .endsWith( "ahps/" ) )
        {
            return this.createWrdsAhpsUri( baseUri, range, featureDetails );
        }
        else if ( baseUri.getPath()
                         .toLowerCase()
                         .contains( "nwm" ) )
        {
            return this.createWrdsNwmUri( baseUri, dataSource, range, featureDetails );
        }
        else
        {
            throw new ProjectConfigException( dataSource.getContext(),
                                              "Unrecognized URI base "
                                              + baseUri );
        }
    }


    /**
     * Specific to USGS API, get a URI for a given issued date range and feature
     *
     * <p>Expecting a USGS URI like this:
     * https://nwis.waterservices.usgs.gov/nwis/iv/</p>
     * @param range the range of dates (from left to right)
     * @param featureDetails the feature to request for
     * @return a URI suitable to get the data from WRDS API
     */

    private URI createUsgsNwisUri( URI baseUri,
                                   Pair<Instant,Instant> range,
                                   FeatureDetails featureDetails,
                                   SortedSet<String> allKnownUsgsGageIds )
    {
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( range );
        Objects.requireNonNull( featureDetails );
        Objects.requireNonNull( allKnownUsgsGageIds );
        Objects.requireNonNull( range.getLeft() );
        Objects.requireNonNull( range.getRight() );

        // example "?format=json&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:0
        if ( !baseUri.getHost()
                     .toLowerCase()
                     .contains( "usgs.gov" ) )
        {
            throw new IllegalArgumentException( "Expected URI like '"
                                                + "https://nwis.waterservices.usgs.gov/nwis/iv"
                                                + " but instead got " + baseUri.toString() );
        }

        if ( Objects.isNull( featureDetails.getGageID() ) )
        {
            return URI.create( "" );
        }

        Map<String, String> urlParameters = createUsgsUrlParameters( range,
                                                                     featureDetails,
                                                                     this.getDataSource(),
                                                                     allKnownUsgsGageIds );
        return getURIWithParameters( this.getBaseUri(),
                                     urlParameters );
    }


    /**
     * Specific to WRDS AHPS API, get URI for given issued date range and feature
     *
     * <p>Expecting a wrds URI like this:
     * http://***REMOVED***.***REMOVED***.***REMOVED***/api/v1/forecasts/streamflow/ahps</p>
     * @param issuedRange the range of issued dates (from left to right)
     * @param featureDetails the feature to request for
     * @return a URI suitable to get the data from WRDS API
     */

    private URI createWrdsAhpsUri( URI baseUri,
                                   Pair<Instant,Instant> issuedRange,
                                   FeatureDetails featureDetails )
    {
        if ( !baseUri.getPath()
                     .toLowerCase()
                     .endsWith( "ahps" ) &&
             !baseUri.getPath()
                     .toLowerCase()
                     .endsWith( "ahps/" ) )
        {
            throw new IllegalArgumentException( "Expected URI like '" +
                                                "http://***REMOVED***.***REMOVED***.***REMOVED***/api/v1/forecasts/streamflow/ahps'"
                                                + " but instead got " + baseUri.toString() );
        }

        String basePath = baseUri.getPath();

        // Tolerate either a slash at end or not.
        if ( !basePath.endsWith( "/" ) )
        {
            basePath = basePath + "/";
        }

        Map<String, String> wrdsParameters = createWrdsAhpsUrlParameters( issuedRange );
        String pathWithLocation = basePath + "nwsLocations/"
                                  + featureDetails.getLid();
        URIBuilder uriBuilder = new URIBuilder( this.getBaseUri() );
        uriBuilder.setPath( pathWithLocation );

        URI uriWithLocation;
        try
        {
            uriWithLocation = uriBuilder.build();
        }
        catch ( URISyntaxException use )
        {
            throw new IllegalArgumentException( "Could not create URI from "
                                                + this.getBaseUri().toString()
                                                + " and "
                                                + pathWithLocation, use );
        }

        return getURIWithParameters( uriWithLocation,
                                     wrdsParameters );
    }


    /**
     * Specific to WRDS NWM API, create a URI for nwm forecasts with given range
     * @param baseUri Base URI, a URI with missing parameters to be added.
     * @param dataSource The data source information, because variable needed.
     * @param issuedRange The datetime range of nwm reference datetimes to set.
     * @param featureDetails The WRES features used in this evaluation.
     * @return A URI with full path and parameters to get NWM from WRDS NWM API.
     */

    private URI createWrdsNwmUri( URI baseUri,
                                  DataSource dataSource,
                                  Pair<Instant, Instant> issuedRange,
                                  FeatureDetails featureDetails )
    {
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( issuedRange );
        Objects.requireNonNull( featureDetails );

        if ( !baseUri.getPath()
                     .toLowerCase()
                     .contains( "nwm" ) )
        {
            throw new IllegalArgumentException( "Expected URI like '" +
                                                "http://***REMOVED***.***REMOVED***.***REMOVED***/api/v1/nwm/ops'"
                                                + " but instead got " + baseUri.toString() );
        }

        String variableName = dataSource.getVariable()
                                        .getValue();

        String basePath = baseUri.getPath();

        // Tolerate either a slash at end or not.
        if ( !basePath.endsWith( "/" ) )
        {
            basePath = basePath + "/";
        }

        boolean isEnsemble = dataSource.getContext()
                                       .getType()
                                       .equals( DatasourceType.ENSEMBLE_FORECASTS );

        Map<String, String> wrdsParameters = createWrdsAhpsNwmParameters( issuedRange,
                                                                          isEnsemble );
        String pathWithLocation = basePath + variableName + "/nwm_feature_id/"
                                  + featureDetails.getComid() + "/";
        URIBuilder uriBuilder = new URIBuilder( this.getBaseUri() );
        uriBuilder.setPath( pathWithLocation );

        URI uriWithLocation;
        try
        {
            uriWithLocation = uriBuilder.build();
        }
        catch ( URISyntaxException use )
        {
            throw new IllegalArgumentException( "Could not create URI from "
                                                + this.getBaseUri().toString()
                                                + " and "
                                                + pathWithLocation, use );
        }

        return getURIWithParameters( uriWithLocation,
                                     wrdsParameters );
    }

    /**
     * Specific to USGS NWIS API, get date range url parameters
     * @param range the date range to set parameters for
     * @return the key/value parameters
     * @throws NullPointerException When arg or value enclosed inside arg is null
     */

    private Map<String,String> createUsgsUrlParameters( Pair<Instant,Instant> range,
                                                        FeatureDetails featureDetails,
                                                        DataSource dataSource,
                                                        SortedSet<String> allKnownUsgsGageIds )
    {
        LOGGER.trace( "Called createUsgsUrlParameters with {}, {}, {}",
                      range, featureDetails, dataSource );
        Objects.requireNonNull( range );
        Objects.requireNonNull( featureDetails );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( range.getLeft() );
        Objects.requireNonNull( range.getRight() );
        Objects.requireNonNull( featureDetails.getGageID() );
        Objects.requireNonNull( dataSource.getVariable() );
        Objects.requireNonNull( dataSource.getVariable().getValue() );

        SortedSet<String> sites = this.getCompanionUsgsGageIds( featureDetails,
                                                                allKnownUsgsGageIds );

        StringJoiner siteJoiner = new StringJoiner( "," );

        for ( String site: sites )
        {
            siteJoiner.add( site );
        }

        // The start datetime needs to be one second later than the next
        // week-range's end datetime or we end up with duplicates.
        // This follows the "pools are inclusive/exclusive" convention of WRES.
        // For some reason, 1 to 999 milliseconds are not enough.
        Instant startDateTime = range.getLeft()
                                     .plusSeconds( 1 );

        return Map.of( "format", "json",
                       "parameterCd", dataSource.getVariable().getValue(),
                       "startDT", startDateTime.toString(),
                       "endDT", range.getRight().toString(),
                       "sites", siteJoiner.toString() );
    }


    /**
     * Specific to WRDS API, get date range url parameters
     * @param issuedRange the date range to set parameters for
     * @return the key/value parameters
     */

    private Map<String,String> createWrdsAhpsUrlParameters( Pair<Instant,Instant> issuedRange )
    {
        Map<String,String> urlParameters = new HashMap<>();
        urlParameters.put( "issuedTime", "[" + issuedRange.getLeft().toString()
                                         + "," + issuedRange.getRight().toString()
                                         + "]" );
        urlParameters.put( "validTime", "all" );
        return Collections.unmodifiableMap( urlParameters );
    }


    /**
     * Specific to WRDS API, get date range url parameters
     * @param issuedRange the date range to set parameters for
     * @return the key/value parameters
     */

    private Map<String,String> createWrdsAhpsNwmParameters( Pair<Instant,Instant> issuedRange,
                                                            boolean isEnsemble )
    {
        Map<String,String> urlParameters = new HashMap<>();
        String leftWrdsFormattedDate = iso8601TruncatedToHourFromInstant( issuedRange.getLeft() );
        String rightWrdsFormattedDate = iso8601TruncatedToHourFromInstant( issuedRange.getRight() );
        urlParameters.put( "reference_time", "(" + leftWrdsFormattedDate
                                         + "," + rightWrdsFormattedDate
                                         + "]" );
        urlParameters.put( "validTime", "all" );

        if ( isEnsemble )
        {
            urlParameters.put( "forecast_type", "ensemble" );
        }
        else
        {
            urlParameters.put( "forecast_type", "deterministic" );
        }

        return Collections.unmodifiableMap( urlParameters );
    }


    /**
     * WRDS NWM API uses a concise ISO-8601 format rather than RFC3339 instant.
     * @param instant
     * @return
     */
    private String iso8601TruncatedToHourFromInstant( Instant instant )
    {
        String dateFormat = "uuuuMMdd'T'HHX";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( dateFormat )
                                                       .withZone( ZoneId.of( "UTC" ) );
        return formatter.format( instant );
    }

    /**
     * Get a URI based on given URI but with urlParameters set.
     *
     * <p>Not specific to a particular API.</p>
     * @param uri the uri to build upon
     * @param urlParameters the parameters to add to the uri
     * @return the uri with the urlParameters added, in repeatable/sorted order.
     * @throws NullPointerException When any argument is null.
     */

    private URI getURIWithParameters( URI uri, Map<String,String> urlParameters )
    {
        Objects.requireNonNull( uri );
        Objects.requireNonNull( urlParameters );

        URIBuilder uriBuilder = new URIBuilder( uri );
        SortedMap<String,String> sortedUrlParameters = new TreeMap<>( urlParameters );

        for ( Map.Entry<String,String> parameter : sortedUrlParameters.entrySet() )
        {
            uriBuilder.setParameter( parameter.getKey(), parameter.getValue() );
        }

        try
        {
            URI finalUri = uriBuilder.build();
            LOGGER.debug( "Created URL {}", finalUri );
            return finalUri;
        }
        catch ( URISyntaxException e )
        {
            throw new IllegalArgumentException( "Could not create URI from "
                                                + uri.toString() + " and "
                                                + urlParameters.toString(), e );
        }
    }


    /**
     * Given a WRES feature with a USGS gage id, get list of companion USGS ids.
     *
     * <p>Specific to use of USGS NWIS API.</p>
     *
     * <p>The use of this is solely for NWIS retrieval. NWIS has several hundred
     * milliseconds of waiting time per request, regardless of the count of gage
     * ids in the request, so to speed things up, we want to bundle gages
     * into a request. But we also want to bundle the same gages because one
     * request will become one source, so that re-use across evaluations can
     * happen, slowing the growth of the database.</p>
     * @param featureDetails The feature with a USGS gage id to find companions.
     * @param allKnownUsgsGageIds A sorted set of all known USGS gage ids.
     * @return USGS gage ids that are companions to the passed featureDetails,
     * no more than 100 elements, includes the gage id passed in.
     * @throws NullPointerException When arg is null or gage id in it is null.
     * @throws PreIngestException When md5sum is unavailable.
     */

    private SortedSet<String> getCompanionUsgsGageIds( FeatureDetails featureDetails,
                                                       SortedSet<String> allKnownUsgsGageIds )
    {
        Objects.requireNonNull( featureDetails );
        Objects.requireNonNull( allKnownUsgsGageIds );
        Objects.requireNonNull( featureDetails.getGageID() );

        String gageId = featureDetails.getGageID();

        MessageDigest md5Name;

        try
        {
            md5Name = MessageDigest.getInstance( "MD5" );
        }
        catch ( NoSuchAlgorithmException nsae )
        {
            throw new PreIngestException( "Couldn't use MD5 algorithm.", nsae );
        }

        DigestUtils digestUtils = new DigestUtils( md5Name );
        byte[] hash = digestUtils.digest( gageId );

        if ( hash.length < 16 )
        {
            throw new PreIngestException( "The MD5 sum of " + gageId
                                          + " was shorter than expected." );
        }

        LOGGER.debug( "Hash of gageId {} is {}", gageId, hash );

        // This only happens to work because we have around 10,000 gages known.
        // This means we'll have around 30-50 gages per leading 256 bits.
        // If we were to have more than 20,000 gages, we risk having > 100 and
        // would need to use the third digit too to reduce the chances of > 100.
        // Why do we need < 100? Because USGS NWIS will reject the request for
        // more than 100 gages at a time.

        SortedSet<String> companions = new TreeSet<>();

        for ( String someGageId : allKnownUsgsGageIds )
        {
            DigestUtils someGageDigest = new DigestUtils( md5Name );
            byte[] someGageIdHash = someGageDigest.digest( someGageId );

            LOGGER.debug( "Hash of someGageId {} is {}", someGageId,
                          someGageIdHash );

            if ( someGageIdHash[0] == hash[0] )
            {
                companions.add( someGageId );
            }
        }

        if ( companions.size() > 100 && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Expected 100 or fewer gages as companions of gage id {} but got {}. A request to NWIS may fail, sites: {}",
                         gageId, companions.size(), companions );
        }

        return Collections.unmodifiableSortedSet( companions );
    }


    /**
     * Low level function that probably does not belong here but is only used
     * here, so here it is.
     * @return The sorted set of all known-to-WRES-db usgs gage ids.
     * @throws PreIngestException When communication with the database fails.
     */

    private SortedSet<String> getAllKnownUsgsGageIds()
    {
        SortedSet<String> gageIds = new TreeSet<>();

        // Avoiding regex in query due to dbms implementation differences.
        DataScripter script = new DataScripter( "SELECT gage_id "
                                                + "FROM wres.Feature "
                                                + "WHERE CHARACTER_LENGTH( gage_id ) >= 8 " );

        try
        {
            try ( DataProvider data = script.getData() )
            {
                while ( data.next() )
                {
                    String gageId = data.getString( "gage_id" );

                    // We want only ascii 0 through 9 in gages ids.
                    IntPredicate asciiNumeric = i -> i >= 48 && i <= 57;
                    if ( gageId.strip()
                               .chars()
                               .allMatch( asciiNumeric ) )
                    {
                        gageIds.add( gageId );
                    }
                    else
                    {
                        LOGGER.warn( "Invalid USGS gage_id in WRES feature table: {}",
                                     gageId );
                    }
                }
            }
        }
        catch ( SQLException se )
        {
            throw new PreIngestException( "Failed to communicate with database when getting usgs gage ids.",
                                          se );
        }

        return Collections.unmodifiableSortedSet( gageIds );
    }
}
