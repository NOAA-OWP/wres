package wres.io.reading;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
import wres.config.generated.DateCondition;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.IngestSaver;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.system.SystemSettings;
import wres.util.Strings;

/**
 * Takes a single web source and splits it into week-long chunks, creates an
 * ingest task for each chunk, GETs chunks of data several at a time, ingests
 * them, and returns the results.
 */
class WebSource implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebSource.class );

    private static final String ISSUED_DATES_ERROR_MESSAGE =
            "One must specify issued dates with both earliest and latest (e.g. "
            + "<issuedDates earliest=\"2018-12-28T15:42:00Z\" "
            + "latest=\"2019-01-01T00:00:00Z\" />) when using a web API as a "
            + "source for forecasts.";

    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
    private final DataSourceConfig.Source sourceConfig;
    private final URI baseUri;
    private final OffsetDateTime now;

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingResults;

    static WebSource of( ProjectConfig projectConfig,
                         DataSourceConfig dataSourceConfig,
                         DataSourceConfig.Source sourceConfig )
    {
        return new WebSource( projectConfig,
                              dataSourceConfig,
                              sourceConfig,
                              OffsetDateTime.now() );
    }

    WebSource( ProjectConfig projectConfig,
               DataSourceConfig dataSourceConfig,
               DataSourceConfig.Source sourceConfig,
               OffsetDateTime now )
    {
        this.projectConfig = projectConfig;
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.baseUri = sourceConfig.getValue();

        if ( this.baseUri.getScheme() == null
             || !this.baseUri.getScheme().startsWith( "http" ) )
        {
            throw new IllegalArgumentException( "URI " + this.baseUri.toString()
                                                + " does not appear to be a web source." );
        }

        ThreadFactory webClientFactory = new BasicThreadFactory.Builder()
                .namingPattern( "WebSource Ingest Executor" )
                .build();

        // Because we use a latch and queue below, no need to make this queue
        // any larger than that queue and latch.
        BlockingQueue<Runnable> webClientQueue = new ArrayBlockingQueue<>( SystemSettings.getMaximumWebClientThreads() );
        this.ingestSaverExecutor = new ThreadPoolExecutor( SystemSettings.getMaximumWebClientThreads(),
                                                           SystemSettings.getMaximumWebClientThreads(),
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
        this.ingests = new ArrayBlockingQueue<>( SystemSettings.getMaximumWebClientThreads() );
        // The size of this latch is for reason (2) above
        this.startGettingResults = new CountDownLatch( SystemSettings.getMaximumWebClientThreads() );
        this.now = now;
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private DataSourceConfig getDataSourceConfig()
    {
        return this.dataSourceConfig;
    }

    private DataSourceConfig.Source getSourceConfig()
    {
        return this.sourceConfig;
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

        Set<Pair<Instant,Instant>> issuedRanges = createWeekRanges( this.getProjectConfig(),
                                                                    this.getNow() );

        try
        {
            for ( FeatureDetails featureDetails : features )
            {
                for ( Pair<Instant, Instant> issuedRange : issuedRanges )
                {
                    URI wrdsUri = createWrdsUri( this.getBaseUri(),
                                                 issuedRange,
                                                 featureDetails );

                    // TODO: hash contents, not the URL
                    String hash = Strings.getMD5Checksum( wrdsUri.toString()
                                                                 .getBytes() );
                    IngestSaver ingestSaver =
                            IngestSaver.createTask()
                                       .withFilePath( wrdsUri )
                                       .withProject( this.getProjectConfig() )
                                       .withDataSourceConfig( this.getDataSourceConfig() )
                                       .withSourceConfig( this.getSourceConfig() )
                                       .withHash( hash )
                                       .isRemote()
                                       .build();

                    Future<List<IngestResult>> future =
                            this.getIngestSaverExecutor()
                                .submit( ingestSaver );
                    this.ingests.add( future );

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
                                                         OffsetDateTime nowDate )
    {
        Objects.requireNonNull( config );
        Objects.requireNonNull( config.getPair() );

        if ( config.getPair().getIssuedDates() == null )
        {
            throw new ProjectConfigException( config.getPair(),
                                              ISSUED_DATES_ERROR_MESSAGE );
        }

        DateCondition issuedDates = config.getPair().getIssuedDates();

        if ( issuedDates.getEarliest() == null
             || issuedDates.getLatest() == null )
        {
            throw new ProjectConfigException( issuedDates,
                                              ISSUED_DATES_ERROR_MESSAGE );
        }

        Set<Pair<Instant,Instant>> weekRanges = new HashSet<>();

        OffsetDateTime earliest;
        String specifiedEarliest = issuedDates.getEarliest();

        OffsetDateTime latest;
        String specifiedLatest = issuedDates.getLatest();

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
     * Specific to WRDS API, get a URI for a given issued date range and feature
     *
     * <p>Expecting a wrds URI like this:
     * http://***REMOVED***.***REMOVED***.***REMOVED***/api/v1/forecasts/streamflow/ahps</p>
     * @param issuedRange the range of issued dates (from left to right)
     * @param featureDetails the feature to request for
     * @return a URI suitable to get the data from WRDS API
     */

    private URI createWrdsUri( URI baseUri,
                               Pair<Instant,Instant> issuedRange,
                               FeatureDetails featureDetails )
    {
        if ( !baseUri.getPath().endsWith( "ahps" ) &&
             !baseUri.getPath().endsWith( "ahps/" ) )
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

        Map<String, String> wrdsParameters = createWrdsUrlParameters( issuedRange );
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
     * Specific to WRDS API, get date range url parameters
     * @param issuedRange the date range to set parameters for
     * @return the key/value parameters
     */

    private Map<String,String> createWrdsUrlParameters( Pair<Instant,Instant> issuedRange )
    {
        Map<String,String> urlParameters = new HashMap<>();
        urlParameters.put( "issuedTime", "[" + issuedRange.getLeft().toString()
                                         + "," + issuedRange.getRight().toString()
                                         + "]" );
        urlParameters.put( "validTime", "all" );
        return Collections.unmodifiableMap( urlParameters );
    }


    /**
     * Get a URI based on given URI but with urlParameters set.
     *
     * <p>Not specific to a particular API.</p>
     * @param uri the uri to build upon
     * @param urlParameters the parameters to add to the uri
     * @return the uri with the urlParameters added
     */
    private URI getURIWithParameters( URI uri, Map<String,String> urlParameters )
    {
        URIBuilder uriBuilder = new URIBuilder( uri );

        for ( Map.Entry<String,String> parameter : urlParameters.entrySet() )
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
}
