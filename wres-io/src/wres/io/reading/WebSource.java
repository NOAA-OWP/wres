package wres.io.reading;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

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
import wres.util.Strings;

/**
 * Takes a single web source and splits it into week-long chunks, creates an
 * ingest task for each chunk.
 */
public class WebSource implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebSource.class );

    private final ProjectConfig projectConfig;
    private final DataSourceConfig dataSourceConfig;
    private final DataSourceConfig.Source sourceConfig;
    private final URI baseUri;

    public WebSource( ProjectConfig projectConfig,
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
    }

    @Override
    public List<IngestResult> call() throws IOException
    {
        List<IngestResult> ingestResults = new ArrayList<>();
        List<Pair<Instant,Instant>> issuedRanges = createWeekRanges( this.projectConfig );

        for ( Pair<Instant,Instant> issuedRange : issuedRanges )
        {
            // TODO: incorporate location
            Map<String, String> wrdsParameters = createWrdsUrlParameters( issuedRange );
            URI wrdsURI = getURIWithParameters( this.baseUri, wrdsParameters );

            // TODO: hash contents, not the URL
            String hash = Strings.getMD5Checksum( wrdsURI.toString().getBytes() );
            IngestSaver ingestSaver = IngestSaver.createTask()
                                                 .withFilePath( wrdsURI )
                                                 .withProject( this.projectConfig )
                                                 .withDataSourceConfig( this.dataSourceConfig )
                                                 .withSourceConfig( this.sourceConfig )
                                                 .withHash( hash )
                                                 .isRemote()
                                                 .build();
            List<IngestResult> ingestResult = ingestSaver.execute();
            ingestResults.addAll( ingestResult );
        }

        return Collections.unmodifiableList( ingestResults );
    }


    /**
     * Break up dates into weeks starting at T00Z Sunday and ending T00Z the
     * next sunday. Ingest a week based on issued dates, let the retrieval
     * filter further by valid dates. We are here going to get a superset of
     * what is needed when the user specified issued dates.
     *
     * In case of issued dates not being specified, a naive approach would be
     * to get four more weeks cushion on either side and hope we captured all.
     * Really we should do a walk by week until we get at least 1 forecast and
     * no forecasts in that week contain valid dates in the range specified by
     * the user.
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
            }

            LOGGER.debug( "Calculated ranges {}", weekRanges );
        }
        else
        {
            throw new UnsupportedOperationException( "Must specify <issuedDates earliest=\"...\"> when using web APIs." );
        }

        return Collections.unmodifiableList( weekRanges );
    }

    private Map<String,String> createWrdsUrlParameters( Pair<Instant,Instant> issuedRange )
    {
        Map<String,String> urlParameters = new HashMap<>();

        String issuedDateKey = "issuedTime";
        urlParameters.put( issuedDateKey, "[" + issuedRange.getLeft().toString()
                                          + "," + issuedRange.getRight().toString()
                                          + "]" );
        urlParameters.put( "validTime", "all" );
        return Collections.unmodifiableMap( urlParameters );
    }

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
