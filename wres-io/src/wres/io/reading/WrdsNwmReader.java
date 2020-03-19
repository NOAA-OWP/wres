package wres.io.reading;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.io.concurrency.TimeSeriesIngester.GEO_ID_TYPE.LID;

import wres.config.generated.DatasourceType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Features;
import wres.io.data.details.FeatureDetails;
import wres.io.reading.wrds.ReadValueManager;
import wres.io.reading.wrds.nwm.NwmDataPoint;
import wres.io.reading.wrds.nwm.NwmFeature;
import wres.io.reading.wrds.nwm.NwmForecast;
import wres.io.reading.wrds.nwm.NwmMember;
import wres.io.reading.wrds.nwm.NwmRootDocument;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * Reads and ingests NWM data from WRDS NWM API.
 *
 * One per NWM URI to ingest. Creates and submits multiple TimeSeriesIngester
 * instances.
 *
 * One-time use:
 * On construction, creates internal executors.
 * On first call, shuts down its internal executor.
 */

public class WrdsNwmReader implements Callable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReader.class );
    private static Pair<SSLContext, X509TrustManager> SSL_CONTEXT
            = ReadValueManager.getSslContextTrustingDodSigner();

    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT,
                                                               true );
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingIngestResults;

    public WrdsNwmReader(  ProjectConfig projectConfig,
                           DataSource dataSource,
                           DatabaseLockManager lockManager )
    {
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.lockManager = lockManager;

        DatasourceType type = dataSource.getContext()
                                        .getType();

        // Yucky brittle check, remove when the type declaration is removed.
        if ( this.getUri()
                 .getPath()
                 .toLowerCase()
                 .contains( "short_range" ) )
        {
            if ( !type.equals( DatasourceType.SINGLE_VALUED_FORECASTS )
                 && !type.equals( DatasourceType.ANALYSES )
                 && !type.equals( DatasourceType.SIMULATIONS )
                 && !type.equals( DatasourceType.OBSERVATIONS ) )
            {
                throw new UnsupportedOperationException(
                        ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(),
                                                               this.getDataSource()
                                                                   .getContext() )
                        + " source specified type '"
                        + type.value()
                        + "' but the word 'short_range' appeared in the URI. "
                        + "You probably want 'single valued forecasts' type or "
                        + "to change the source URI to point to medium_range." );
            }
        }
        else if ( this.getUri()
                      .getPath()
                      .toLowerCase()
                      .contains( "medium_range" ) )
        {
            if ( !type.equals( DatasourceType.ENSEMBLE_FORECASTS )
                 && !type.equals( DatasourceType.SINGLE_VALUED_FORECASTS ) )
            {
                throw new UnsupportedOperationException(
                        ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(),
                                                               this.getDataSource()
                                                                   .getContext() )
                        + " source specified type '"
                        + type.value()
                        + "' but the word 'medium_range' appeared in the URI. "
                        + "You probably want 'ensemble forecasts' type or to "
                        + "change the source URI to point to another type." );
            }

            if ( type.equals( DatasourceType.SINGLE_VALUED_FORECASTS ) )
            {
                LOGGER.warn( "{}{}{}{}",
                             "Evaluating only the deterministic medium range ",
                             "forecast because 'single valued forecasts' was ",
                             "declared. To evaluate the ensemble forecast, ",
                             "declare 'ensemble forecasts'." );
            }
        }

        // See comments in wres.io.reading.WebSource for info on below approach.
        ThreadFactory wrdsNwmReaderIngest = new BasicThreadFactory.Builder()
                .namingPattern( "WrdsNwmReader Ingest" )
                .build();

        // As of 2.1, the SystemSetting is used in two different NWM readers:
        int concurrentCount = SystemSettings.getMaxiumNwmIngestThreads();
        BlockingQueue<Runnable> webClientQueue = new ArrayBlockingQueue<>( concurrentCount );
        this.ingestSaverExecutor = new ThreadPoolExecutor( concurrentCount,
                                                           concurrentCount,
                                                           SystemSettings.poolObjectLifespan(),
                                                           TimeUnit.MILLISECONDS,
                                                           webClientQueue,
                                                           wrdsNwmReaderIngest );
        this.ingestSaverExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
        this.ingests = new ArrayBlockingQueue<>( concurrentCount );
        this.startGettingIngestResults = new CountDownLatch( concurrentCount );
        LOGGER.debug( "Created WrdsNwmReader for {}", this.dataSource );
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
        return WrdsNwmReader.JSON_OBJECT_MAPPER;
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
        URI uri = this.getUri();
        InputStream dataStream = null;

        try
        {
            Pair<Integer,InputStream> response = WEB_CLIENT.getFromWeb( uri );
            int responseStatus = response.getLeft();
            dataStream = response.getRight();

            if ( responseStatus >= 400 && responseStatus < 500 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             responseStatus,
                             uri );
                return Collections.emptyList();
            }

            document = this.getJsonObjectMapper()
                           .readValue( dataStream,
                                       NwmRootDocument.class );
            LOGGER.debug( "Parsed this document: {}", document );
        }
        catch ( IOException ioe )
        {
            this.shutdownNow();
            throw new PreIngestException( "Failed to read NWM data from "
                                          + uri,
                                          ioe );
        }
        finally
        {
            if ( Objects.nonNull( dataStream) )
            {
                try
                {
                    dataStream.close();
                }
                catch ( IOException ioe )
                {
                    LOGGER.warn( "Could not close a data stream from {}",
                                 uri, ioe );
                }
            }
        }

        String variableName = document.getVariable()
                                         .get( "name" );
        String measurementUnit = document.getVariable()
                                         .get( "unit" );

        // Is this a hack or not? Translate "meter^3 / sec" to "CMS"
        if ( measurementUnit.equals( "meter^3 / sec") )
        {
            measurementUnit = "CMS";
        }

        try
        {
            int emptyTimeseriesCount = 0;

            for ( NwmForecast forecast : document.getForecasts() )
            {
                for ( NwmFeature nwmFeature : forecast.getFeatures() )
                {
                    Pair<String, TimeSeries<?>> transformed =
                            this.transform( forecast.getReferenceDatetime(),
                                            nwmFeature );
                    String locationName = transformed.getKey();
                    TimeSeries<?> timeSeries = transformed.getValue();

                    if ( !timeSeries.getEvents()
                                    .isEmpty() )
                    {
                        TimeSeriesIngester timeSeriesIngester =
                                this.createTimeSeriesIngester( this.getProjectConfig(),
                                                               this.getDataSource(),
                                                               this.getLockManager(),
                                                               timeSeries,
                                                               locationName,
                                                               variableName,
                                                               measurementUnit );

                        Future<List<IngestResult>> futureIngestResult =
                                this.ingestSaverExecutor.submit(
                                        timeSeriesIngester );
                        this.ingests.add( futureIngestResult );
                        this.startGettingIngestResults.countDown();

                        // See WebSource for comments on this approach.
                        if ( this.startGettingIngestResults.getCount() <= 0 )
                        {
                            Future<List<IngestResult>> future =
                                    this.ingests.take();
                            List<IngestResult> ingestResults = future.get();
                            ingested.addAll( ingestResults );
                        }
                    }
                    else
                    {
                        // Keep track of how many empty timeseries there were
                        // to avoid spamming the log.
                        emptyTimeseriesCount++;
                    }
                }
            }

            if ( emptyTimeseriesCount > 0 )
            {
                LOGGER.warn( "Skipped {} empty timeseries from {}",
                             emptyTimeseriesCount, uri );
            }

            // Finish getting the remainder of ingest results.
            for ( Future<List<IngestResult>> future : this.ingests )
            {
                List<IngestResult> ingestResults = future.get();
                ingested.addAll( ingestResults );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while ingesting NWM data from "
                         + uri, ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            throw new IngestException( "Failed to ingest NWM data from "
                                       + uri, ee );
        }
        finally
        {
            this.shutdownNow();
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
        String wresGenericFeatureName = this.getWresFeatureNameFromNwmFeatureId( rawLocationId );
        NwmMember[] members = feature.getMembers();
        TimeSeries<?> timeSeries;

        if ( members.length == 1 )
        {
            // Infer that these are single-valued data.
            SortedSet<Event<Double>> events = new TreeSet<>();

            for ( NwmDataPoint dataPoint : members[0].getDataPoints() )
            {
                if ( Objects.isNull( dataPoint ) )
                {
                    LOGGER.debug( "Found null datapoint in sole trace at referenceDatetime={} for nwm feature={}",
                                 referenceDatetime, rawLocationId );
                    continue;
                }

                Event<Double> event = Event.of( dataPoint.getTime(),
                                                dataPoint.getValue() );
                events.add( event );
            }

            ReferenceTimeType referenceTimeType = ReferenceTimeType.T0;

            // Special rule: when analysis data is found, reference time not T0.
            if ( this.getUri()
                     .getPath()
                     .toLowerCase()
                     .contains( "analysis" ) )
            {
                referenceTimeType = ReferenceTimeType.ANALYSIS_START_TIME;

                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Analysis data found labeled in URI {}",
                                  this.getUri() );
                }
            }

            timeSeries = TimeSeries.of( referenceDatetime,
                                        referenceTimeType,
                                        Collections.unmodifiableSortedSet( events ) );
        }
        else if ( members.length > 1 )
        {
            // Infer that this is ensemble data.
            SortedMap<Instant,double[]> primitiveData = new TreeMap<>();

            // TODO: avoid reading into multiple intermediate containers. Instead, create a
            // TimeSeriesBuilder<Ensemble> and then add each Event<Ensemble> to the builder.
            
            for ( int i = 0; i < members.length; i++ )
            {
                int valueCountInTrace = members[i].getDataPoints().size();

                for ( NwmDataPoint dataPoint : members[i].getDataPoints() )
                {
                    if ( Objects.isNull( dataPoint ) )
                    {
                        LOGGER.debug( "Found null datapoint in member trace={} at referenceDatetime={} for nwm feature={}",
                                      i, referenceDatetime, rawLocationId );
                        continue;
                    }

                    Instant validDatetime = dataPoint.getTime();

                    if ( !primitiveData.containsKey( validDatetime ) )
                    {
                        double[] rawEnsemble = new double[members.length];
                        primitiveData.put( validDatetime, rawEnsemble );
                    }

                    double[] rawEnsemble = primitiveData.get( validDatetime );
                    rawEnsemble[i] = dataPoint.getValue();
                }

                // Special case of zero data in the trace means skip the others.
                if ( primitiveData.keySet().isEmpty() )
                {
                    LOGGER.warn( "Empty ensemble trace found in member trace index={} at reference datetime={} for NWM feature={}, skipping the remaining traces.",
                                 i, referenceDatetime, rawLocationId );
                    break;
                }
                else if ( primitiveData.keySet().size() != valueCountInTrace )
                {
                    throw new PreIngestException( "Data from "
                                                  + this.getUri()
                                                  + " in forecast member "
                                                  + members[i].getIdentifier()
                                                  + " had value count "
                                                  + valueCountInTrace
                                                  + " but the cumulative count "
                                                  + "of datetimes so far was "
                                                  + primitiveData.keySet().size()
                                                  + ". Therefore one member is "
                                                  + "of different length than "
                                                  + "another, different valid "
                                                  + "datetimes were found, or "
                                                  + "duplicate datetimes were "
                                                  + "found. Any of these cases "
                                                  + "means an invalid ensemble "
                                                  + "was found." );
                }
            }

            // Re-shape the data to match the WRES metrics/datamodel expectation
            SortedSet<Event<Ensemble>> data = new TreeSet<>();

            for ( Map.Entry<Instant,double[]> row : primitiveData.entrySet() )
            {
                Ensemble ensemble = Ensemble.of( row.getValue() );
                Event<Ensemble> ensembleEvent = Event.of( row.getKey(), ensemble );
                data.add( ensembleEvent );
            }

            timeSeries = TimeSeries.of( referenceDatetime,
                                        ReferenceTimeType.T0,
                                        data );
        }
        else
        {
            // There are fewer than 1 members.
            throw new PreIngestException( "No members found in WRDS NWM data" );
        }

        return Pair.of( wresGenericFeatureName,
                        timeSeries );
    }

    String getWresFeatureNameFromNwmFeatureId( int rawLocationId )
    {
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
            throw new PreIngestException( "Unable to transform raw NWM feature "
                                          + " id " + rawLocationId
                                          + " into WRES Feature:", se );
        }

        return featureDetailsFromKey.getLid();
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @return a TimeSeriesIngester
     */

    TimeSeriesIngester createTimeSeriesIngester( ProjectConfig projectConfig,
                                                 DataSource dataSource,
                                                 DatabaseLockManager lockManager,
                                                 TimeSeries<?> timeSeries,
                                                 String locationName,
                                                 String variableName,
                                                 String measurementUnit )
    {
        return new TimeSeriesIngester( projectConfig,
                                       dataSource,
                                       lockManager,
                                       timeSeries,
                                       locationName,
                                       LID,
                                       variableName,
                                       measurementUnit );
    }

    /**
     * Shuts down executors, logs timing information if present.
     */
    private void shutdownNow()
    {
        List<Runnable> abandoned = this.ingestSaverExecutor.shutdownNow();

        if ( !abandoned.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Abandoned {} ingest tasks for reader of URI {}",
                         abandoned.size(), this.getUri() );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "{}", WEB_CLIENT.getTimingInformation() );
        }
    }
}
