package wres.io.reading;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.reading.wrds.nwm.NwmDataPoint;
import wres.io.reading.wrds.nwm.NwmFeature;
import wres.io.reading.wrds.nwm.NwmForecast;
import wres.io.reading.wrds.nwm.NwmMember;
import wres.io.reading.wrds.nwm.NwmRootDocument;
import wres.system.DatabaseLockManager;

/**
 * Reads and ingests NWM data from WRDS NWM API.
 *
 * One per NWM URI to ingest. Creates and submits multiple TimeSeriesIngester
 * instances.
 *
 * Work in progress as of 2020-01-14.
 */

public class WrdsNwmReader implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReader.class );
    private static final WebClient WEB_CLIENT = new WebClient();
    private final ObjectMapper jsonObjectMapper;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;

    public WrdsNwmReader(  ProjectConfig projectConfig,
                           DataSource dataSource,
                           DatabaseLockManager lockManager )
    {
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
        this.jsonObjectMapper = new ObjectMapper()
                .registerModule( new JavaTimeModule() );
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private DataSource getDataSource()
    {
        return this.dataSource;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    private ObjectMapper getJsonObjectMapper()
    {
        return this.jsonObjectMapper;
    }

    private URI getUri()
    {
        return this.getDataSource()
                   .getUri();
    }

    @Override
    public List<IngestResult> call() throws IngestException
    {
        List<IngestResult> ingested = new ArrayList<>();
        NwmRootDocument document;

        try
        {
            InputStream dataStream = WEB_CLIENT.getFromWeb( this.getUri() )
                                               .getRight();
            document = this.getJsonObjectMapper()
                           .readValue( dataStream,
                                       NwmRootDocument.class );
            LOGGER.info( "Parsed this document: {}", document );
        }
        catch ( IOException ioe )
        {
            throw new PreIngestException( "Failed to read NWM data from "
                                          + this.getUri(),
                                          ioe );
        }

        String variableName = document.getVariable()
                                         .get( "name" );
        String measurementUnit = document.getVariable()
                                         .get( "unit" );

        // Is this a hack or not? Translate "meter^3 / sec" to "m3/s"
        if ( measurementUnit.equals( "meter^3 / sec") )
        {
            measurementUnit = "m3/s";
        }

        for ( NwmForecast forecast : document.getForecasts() )
        {
            for ( NwmFeature nwmFeature : forecast.getFeatures() )
            {
                Pair<String, TimeSeries<?>> transformed =
                        this.transform( forecast.getReferenceDatetime(),
                                        nwmFeature );
                String locationName = transformed.getKey();
                TimeSeries<?> timeSeries = transformed.getValue();

                // TODO: ingest wres timeseries concurrently
                TimeSeriesIngester timeSeriesIngester =
                        new TimeSeriesIngester( this.getProjectConfig(),
                                                this.getDataSource(),
                                                this.getLockManager(),
                                                timeSeries,
                                                locationName,
                                                variableName,
                                                measurementUnit );

                try
                {
                    List<IngestResult> ingestResults = timeSeriesIngester.call();
                    ingested.addAll( ingestResults );
                }
                catch ( IOException ioe )
                {
                    throw new IngestException( "Failed to ingest data from "
                                               + this.getUri()
                                               + " with location  "
                                               + locationName, ioe );
                }
            }
        }

        return Collections.unmodifiableList( ingested );
    }


    /**
     * Transform deserialized JSON document (now a POJO tree) to TimeSeries.
     * @param feature The POJO with a TimeSeries in it.
     * @return The NWM location name (akd nwm feature id, comid) and TimeSeries.
     */

    private Pair<String,TimeSeries<?>> transform( Instant referenceDatetime,
                                                  NwmFeature feature )
    {
        Objects.requireNonNull( feature );
        Objects.requireNonNull( feature.getLocation() );
        Objects.requireNonNull( feature.getLocation().getNwmLocationNames() );

        int rawLocationId = feature.getLocation()
                                   .getNwmLocationNames()
                                   .getNwmFeatureId();
        FeatureDetails featureDetailsFromKey;
        Feature featureWithComid =  new Feature( null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 ( long ) rawLocationId,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null,
                                                 null );
        try
        {
            featureDetailsFromKey = Features.getDetails( featureWithComid );
        }
        catch ( SQLException se )
        {
            throw new PreIngestException( "Unable to transform raw NWM feature id"
                                          + rawLocationId
                                          + " into WRES Feature:", se );
        }

        String wresGenericFeatureName = featureDetailsFromKey.getLid();
        NwmMember[] members = feature.getMembers();
        TimeSeries<?> timeSeries;

        if ( members.length == 1 )
        {
            // Infer that these are single-valued data.
            SortedSet<Event<Double>> events = new TreeSet<>();

            for ( NwmDataPoint dataPoint : members[0].getDataPoints() )
            {
                Event<Double> event = Event.of( dataPoint.getTime(),
                                                dataPoint.getValue() );
                events.add( event );
            }

            timeSeries = TimeSeries.of( referenceDatetime,
                                        ReferenceTimeType.T0,
                                        events );
        }
        else if ( members.length > 1 )
        {
            // Infer that this is ensemble data.
            // TODO transform ensemble data
            throw new UnsupportedOperationException( "Need to implement ensemble data reading" );
        }
        else
        {
            // There are fewer than 1 members.
            throw new PreIngestException( "No members found in WRDS NWM data" );
        }

        return Pair.of( wresGenericFeatureName,
                        timeSeries );
    }
}
