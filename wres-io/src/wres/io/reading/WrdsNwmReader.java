package wres.io.reading;

import java.io.IOException;
import java.net.URI;
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

import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.FeatureKey;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.wrds.ReadValueManager;
import wres.io.reading.wrds.TimeScaleFromParameterCodes;
import wres.io.reading.wrds.nwm.NwmDataPoint;
import wres.io.reading.wrds.nwm.NwmFeature;
import wres.io.reading.wrds.nwm.NwmForecast;
import wres.io.reading.wrds.nwm.NwmMember;
import wres.io.reading.wrds.nwm.NwmRootDocument;
import wres.io.utilities.Database;
import wres.io.utilities.WebClient;
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
    private static final boolean TRACK_TIMINGS = false;
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT,
                                                               TRACK_TIMINGS );
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingIngestResults;

    public WrdsNwmReader( SystemSettings systemSettings,
                          Database database,
                          Features featuresCache,
                          Variables variablesCache,
                          Ensembles ensemblesCache,
                          MeasurementUnits measurementUnitsCache,
                          ProjectConfig projectConfig,
                          DataSource dataSource,
                          DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( variablesCache );
        Objects.requireNonNull( ensemblesCache );
        Objects.requireNonNull( measurementUnitsCache );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );

        this.systemSettings = systemSettings;
        this.database = database;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
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
        }

        // See comments in wres.io.reading.WebSource for info on below approach.
        ThreadFactory wrdsNwmReaderIngest = new BasicThreadFactory.Builder()
                .namingPattern( "WrdsNwmReader Ingest %d" )
                .build();

        // As of 2.1, the SystemSetting is used in two different NWM readers:
        int concurrentCount = systemSettings.getMaxiumNwmIngestThreads();
        BlockingQueue<Runnable> webClientQueue = new ArrayBlockingQueue<>( concurrentCount );
        this.ingestSaverExecutor = new ThreadPoolExecutor( concurrentCount,
                                                           concurrentCount,
                                                           systemSettings.poolObjectLifespan(),
                                                           TimeUnit.MILLISECONDS,
                                                           webClientQueue,
                                                           wrdsNwmReaderIngest );
        this.ingestSaverExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
        this.ingests = new ArrayBlockingQueue<>( concurrentCount );
        this.startGettingIngestResults = new CountDownLatch( concurrentCount );
        LOGGER.debug( "Created WrdsNwmReader for {}", this.dataSource );
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private Variables getVariablesCache()
    {
        return this.variablesCache;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
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

        try (WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri ))
        {
            if ( response.getStatusCode() >= 400 && response.getStatusCode() < 500 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             response.getStatusCode(),
                             uri );
                return Collections.emptyList();
            }

            document = this.getJsonObjectMapper().readValue( response.getResponse(), NwmRootDocument.class );
            LOGGER.debug( "Parsed this document: {}", document );
        }
        catch ( IOException ioe )
        {
            this.shutdownNow();
            throw new PreIngestException( "Failed to read NWM data from "
                                          + uri,
                                          ioe );
        }

        String variableName = document.getVariable().get( "name" );
        String measurementUnit = document.getVariable().get( "unit" );
        
        // Time scale if available
        TimeScaleOuter timeScale = null;
 
        if( Objects.nonNull( document.getParameterCodes() ) )
        {
            timeScale = TimeScaleFromParameterCodes.getTimeScale( document.getParameterCodes(), uri );
            LOGGER.debug( "{}{}{}{}",
                         "While processing source ",
                         uri,
                         " discovered a time scale of ",
                         timeScale );
        }
        
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
                    TimeSeries<?> timeSeries =
                            this.transform( forecast.getReferenceDatetime(),
                                            nwmFeature,
                                            timeScale,
                                            variableName,
                                            measurementUnit );

                    if ( !timeSeries.getEvents()
                                    .isEmpty() )
                    {
                        TimeSeriesIngester timeSeriesIngester =
                                this.createTimeSeriesIngester( this.getSystemSettings(),
                                                               this.getDatabase(),
                                                               this.getFeaturesCache(),
                                                               this.getVariablesCache(),
                                                               this.getEnsemblesCache(),
                                                               this.getMeasurementUnitsCache(),
                                                               this.getProjectConfig(),
                                                               this.getDataSource(),
                                                               this.getLockManager(),
                                                               timeSeries );

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
     * @param referenceDatetime The reference datetime.
     * @param nwmFeature The POJO with a TimeSeries in it.
     * @param timeScale the time scale associated with the time series.
     * @param variableName The name of the variable.
     * @param measurementUnit The unit of the variable value.
     * @return The NWM location name (akd nwm feature id, comid) and TimeSeries.
     */

    private TimeSeries<?> transform( Instant referenceDatetime,
                                     NwmFeature nwmFeature,
                                     TimeScaleOuter timeScale,
                                     String variableName,
                                     String measurementUnit )
    {
        Objects.requireNonNull( nwmFeature );
        Objects.requireNonNull( nwmFeature.getLocation() );
        Objects.requireNonNull( nwmFeature.getLocation().getNwmLocationNames() );
        Objects.requireNonNull( variableName );
        Objects.requireNonNull( measurementUnit );

        int rawLocationId = nwmFeature.getLocation()
                                      .getNwmLocationNames()
                                      .getNwmFeatureId();
        NwmMember[] members = nwmFeature.getMembers();
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

            FeatureKey feature = new FeatureKey( Integer.toString( rawLocationId ),
                                                 null,
                                                 null,
                                                 null );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( referenceTimeType, referenceDatetime ),
                                                                 timeScale,
                                                                 variableName,
                                                                 feature,
                                                                 measurementUnit );
            timeSeries = new Builder<Double>().addEvents( Collections.unmodifiableSortedSet( events ) )
                                                        .setMetadata( metadata )
                                                        .build();
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
            FeatureKey feature = new FeatureKey( Integer.toString( rawLocationId ),
                                                 null,
                                                 null,
                                                 null );
            TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, referenceDatetime ),
                                                                 timeScale,
                                                                 variableName,
                                                                 feature,
                                                                 measurementUnit );
            Builder<Ensemble> builder = new Builder<Ensemble>().setMetadata( metadata );

            for ( Map.Entry<Instant, double[]> row : primitiveData.entrySet() )
            {
                Ensemble ensemble = Ensemble.of( row.getValue() );
                Event<Ensemble> ensembleEvent = Event.of( row.getKey(), ensemble );
                builder.addEvent( ensembleEvent );
            }

            timeSeries = builder.build();
        }
        else
        {
            // There are fewer than 1 members.
            throw new PreIngestException( "No members found in WRDS NWM data" );
        }

        return timeSeries;
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @return a TimeSeriesIngester
     */

    TimeSeriesIngester createTimeSeriesIngester( SystemSettings systemSettings,
                                                 Database database,
                                                 Features featuresCache,
                                                 Variables variablesCache,
                                                 Ensembles ensemblesCache,
                                                 MeasurementUnits measurementUnitsCache,
                                                 ProjectConfig projectConfig,
                                                 DataSource dataSource,
                                                 DatabaseLockManager lockManager,
                                                 TimeSeries<?> timeSeries )
    {
        return TimeSeriesIngester.of( systemSettings,
                                       database,
                                       featuresCache,
                                       variablesCache,
                                       ensemblesCache,
                                       measurementUnitsCache,
                                       projectConfig,
                                       dataSource,
                                       lockManager,
                                       timeSeries );
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

        if ( TRACK_TIMINGS && LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "{}", WEB_CLIENT.getTimingInformation() );
        }
    }
}
