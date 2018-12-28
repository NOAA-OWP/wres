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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
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
 * ingest task for each chunk.
 */
class WebSource implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebSource.class );

    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
    private final DataSourceConfig.Source sourceConfig;
    private final URI baseUri;

    private final ThreadPoolExecutor executor;

    WebSource( ProjectConfig projectConfig,
               DataSourceConfig dataSourceConfig,
               DataSourceConfig.Source sourceConfig )
    {
        this.projectConfig = projectConfig;
        this.dataSourceConfig = dataSourceConfig;
        this.sourceConfig = sourceConfig;
        this.baseUri = URI.create( sourceConfig.getValue() );

        if ( this.baseUri.getScheme() == null
             || !this.baseUri.getScheme().startsWith( "http" ) )
        {
            throw new IllegalArgumentException( "URI " + this.baseUri.toString()
                                                + " does not appear to be a web source." );
        }

        ThreadFactory webClientFactory = new BasicThreadFactory.Builder()
                .namingPattern( "WebSource Ingest Executor" )
                .build();

        BlockingQueue<Runnable> webClientQueue = new ArrayBlockingQueue<>( 10
                                                                           * SystemSettings.getMaximumWebClientThreads() );
        this.executor = new ThreadPoolExecutor( SystemSettings.getMaximumWebClientThreads(),
                                                SystemSettings.getMaximumWebClientThreads(),
                                                SystemSettings.poolObjectLifespan(),
                                                TimeUnit.MILLISECONDS,
                                                webClientQueue,
                                                webClientFactory );
        this.executor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
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

    private ThreadPoolExecutor getExecutor()
    {
        return this.executor;
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

        List<Pair<Instant,Instant>> issuedRanges = createWeekRanges( this.getProjectConfig() );

        List<Future<List<IngestResult>>> ingests = new ArrayList<>( features.size()
                                                                    * issuedRanges.size() );

        for ( FeatureDetails featureDetails : features )
        {
            for ( Pair<Instant,Instant> issuedRange : issuedRanges )
            {
                URI wrdsUri = createWrdsUri( this.getBaseUri(),
                                             issuedRange,
                                             featureDetails );

                // TODO: hash contents, not the URL
                String hash = Strings.getMD5Checksum( wrdsUri.toString().getBytes() );
                IngestSaver ingestSaver = IngestSaver.createTask()
                                                     .withFilePath( wrdsUri )
                                                     .withProject( this.getProjectConfig() )
                                                     .withDataSourceConfig( this.getDataSourceConfig() )
                                                     .withSourceConfig( this.getSourceConfig() )
                                                     .withHash( hash )
                                                     .isRemote()
                                                     .build();

                ingests.add( this.getExecutor().submit( ingestSaver ) );
            }
        }

        try
        {
            for ( Future<List<IngestResult>> ingestTask : ingests )
            {
                List<IngestResult> ingested = ingestTask.get();
                ingestResults.addAll( ingested );
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
     * <p>In case of issued dates not being specified, a naive approach would be
     * to get four more weeks cushion on either side and hope we captured all.
     * Really we should do a walk by week until we get at least 1 forecast and
     * no forecasts in that week contain valid dates in the range specified by
     * the user.</p>
     *
     * <p>Not specific to a particular API.</p>
     * @param config
     * @return
     */
    private List<Pair<Instant,Instant>> createWeekRanges( ProjectConfig config )
    {
        if ( config == null || config.getPair() == null )
        {
            return Collections.unmodifiableList( Collections.emptyList() );
        }

        List<Pair<Instant,Instant>> weekRanges = new ArrayList<>();

        DateCondition issuedDates = config.getPair().getIssuedDates();

        if ( issuedDates != null )
        {
            OffsetDateTime earliest;
            String specifiedEarliest = issuedDates.getEarliest();

            if ( specifiedEarliest == null )
            {
                throw new UnsupportedOperationException( "When retrieving from a web API, the <issuedDates earliest=\"...\"> attribute must be specified." );
            }
            else
            {
                earliest = OffsetDateTime.parse( specifiedEarliest )
                                  .with( previousOrSame( SUNDAY ) )
                                         .withHour( 0 )
                                         .withMinute( 0 )
                                         .withSecond( 0 )
                                         .withNano( 0 );
            }

            LOGGER.debug( "Given {} calculated {} for earliest.",
                          specifiedEarliest, earliest );

            OffsetDateTime latest;
            String specifiedLatest = issuedDates.getLatest();

            if ( specifiedLatest == null )
            {
                OffsetDateTime now = OffsetDateTime.now();
                LOGGER.warn( "No latest issued date specified, using {} instead.", now );
                latest = now.with( next( SUNDAY ) );
            }
            else
            {
                latest = OffsetDateTime.parse( specifiedLatest );
            }

            latest = latest.withHour( 0 )
                           .withMinute( 0 )
                           .withSecond( 0 )
                           .withNano( 0 );

            LOGGER.debug( "Given {} calculated {} for latest.",
                          specifiedLatest, latest );

            OffsetDateTime left = earliest;
            OffsetDateTime right = earliest;

            while ( right.isBefore( latest ) )
            {
                right = left.with( next( SUNDAY ) );
                Pair<Instant,Instant> range = Pair.of( left.toInstant(), right.toInstant() );
                LOGGER.debug( "Created range {}", range );
                weekRanges.add( range );
                left = right;
                // TODO: when "now" is the latest, use "now" instead of SUNDAY
            }

            LOGGER.debug( "Calculated ranges {}", weekRanges );
        }
        else
        {
            throw new UnsupportedOperationException( "Must specify <issuedDates earliest=\"...\"> when using web APIs." );
        }

        return Collections.unmodifiableList( weekRanges );
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
     * @param uri
     * @param urlParameters
     * @return
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
