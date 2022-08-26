package wres.io.reading.wrds.nwm;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.stream.Stream;

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
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.Builder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.web.WebClient;
import wres.io.reading.wrds.ReadValueManager;
import wres.io.reading.wrds.TimeScaleFromParameterCodes;
import wres.statistics.generated.Geometry;
import wres.system.SystemSettings;

/**
 * Reads and ingests NWM data from WRDS NWM API.
 *
 * One per NWM URI to ingest.
 *
 * One-time use:
 * On construction, creates internal executors.
 * On first call, shuts down its internal executor.
 */

public class WrdsNwmReader implements Callable<List<IngestResult>>
{
    private static final String NO_MEMBERS_FOUND_IN_WRDS_NWM_DATA = "No members found in WRDS NWM data";
    private static final Logger LOGGER = LoggerFactory.getLogger( WrdsNwmReader.class );
    private static final Pair<SSLContext, X509TrustManager> SSL_CONTEXT;
    
    static
    {
        try
        {
            SSL_CONTEXT = ReadValueManager.getSslContextTrustingDodSignerForWrds();
        }
        catch ( PreIngestException e )
        {
            throw new ExceptionInInitializerError( "Failed to acquire a trust manager for WRDS: " + e );
        }
    }
    
    private static final boolean TRACK_TIMINGS = false;
    private static final WebClient WEB_CLIENT = new WebClient( SSL_CONTEXT,
                                                               TRACK_TIMINGS );
    private static final ObjectMapper JSON_OBJECT_MAPPER =
            new ObjectMapper().registerModule( new JavaTimeModule() )
                              .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true );

    private final DataSource dataSource;
    private final URI uri;

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingIngestResults;
    private final TimeSeriesIngester timeSeriesIngester;

    public WrdsNwmReader( TimeSeriesIngester timeSeriesIngester,
                          DataSource dataSource,
                          SystemSettings systemSettings )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( systemSettings );

        this.dataSource = dataSource;
        this.uri = dataSource.getUri();
        this.timeSeriesIngester = timeSeriesIngester;

        DatasourceType type = dataSource.getContext()
                                        .getType();

        LeftOrRightOrBaseline lrb = dataSource.getLeftOrRightOrBaseline();
        
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
                throw new UnsupportedOperationException( lrb
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
                      .contains( "medium_range" )
                  && !type.equals( DatasourceType.ENSEMBLE_FORECASTS )
                  && !type.equals( DatasourceType.SINGLE_VALUED_FORECASTS ) )
        {
            throw new UnsupportedOperationException( lrb
                                                     + " source specified type '"
                                                     + type.value()
                                                     + "' but the word 'medium_range' appeared in the URI. "
                                                     + "You probably want 'ensemble forecasts' type or to "
                                                     + "change the source URI to point to another type." );
        }

        // Could be an NPE, but the data source is not null and the nullity of the variable is an effect, not a cause
        if ( Objects.isNull( this.dataSource.getVariable() ) )
        {
            throw new IllegalArgumentException( "A variable must be declared for a WRDS NWM source but no "
                                                + "variable was found for the "
                                                + lrb
                                                + " WRDS NWM source with URI: "
                                                + this.dataSource.getUri()
                                                + ". Please declare a variable for all "
                                                + lrb
                                                + " WRDS NWM sources." );
        }

        // See comments in wres.io.reading.WebSource for info on below approach.
        ThreadFactory wrdsNwmReaderIngest = new BasicThreadFactory.Builder().namingPattern( "WrdsNwmReader Ingest %d" )
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

    private ObjectMapper getJsonObjectMapper()
    {
        return WrdsNwmReader.JSON_OBJECT_MAPPER;
    }

    private URI getUri()
    {
        return this.uri;
    }
    
    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }
    
    private static boolean getTrackTimings()
    {
        return TRACK_TIMINGS;
    }

    @Override
    public List<IngestResult> call()
    {
        List<IngestResult> ingested = new ArrayList<>();
        NwmRootDocument document;
        URI innerUri = this.getUri();

        if ( innerUri.getScheme().startsWith( "file" ) )
        {
            document = this.readNwmRootDocumentFromFile( innerUri );
        }
        else
        {
            document = this.readNwmRootDocumentFromWebSource( innerUri );
        }

        if ( Objects.isNull( document ) )
        {
            LOGGER.debug( "Failed to read a root document from {}.", innerUri );
            return Collections.emptyList();
        }

        List<String> wrdsWarnings = document.getWarnings();

        if ( wrdsWarnings != null && !wrdsWarnings.isEmpty() )
        {
            LOGGER.warn( "These warnings were in the document from {}: {}",
                         innerUri,
                         wrdsWarnings );
        }

        Map<String, String> variable = document.getVariable();

        if ( variable == null
             || variable.isEmpty()
             || !variable.containsKey( "name" )
             || !variable.containsKey( "unit" ) )
        {
            throw new PreIngestException( "Invalid document from WRDS (variable"
                                          + " and/or unit missing): check the "
                                          + "WRDS and WRES documentation to "
                                          + "ensure the most up-to-date base "
                                          + "URL is declared in the source tag."
                                          + " The invalid document was from "
                                          + innerUri );
        }

        String variableName = variable.get( "name" );
        String measurementUnit = variable.get( "unit" );

        if ( variableName == null
             || variableName.isBlank()
             || measurementUnit == null
             || measurementUnit.isBlank() )
        {
            throw new PreIngestException( "Invalid document from WRDS (variable"
                                          + " and/or unit value was missing): "
                                          + "check the WRDS and WRES "
                                          + "documentation to ensure the most "
                                          + "up-to-date base URL is declared in"
                                          + " the source tag. The invalid "
                                          + "document was from "
                                          + innerUri );
        }

        // Time scale if available
        TimeScaleOuter timeScale = null;

        if ( Objects.nonNull( document.getParameterCodes() ) )
        {
            timeScale = TimeScaleFromParameterCodes.getTimeScale( document.getParameterCodes(), innerUri );
            LOGGER.debug( "{}{}{}{}",
                          "While processing source ",
                          innerUri,
                          " discovered a time scale of ",
                          timeScale );
        }

        try
        {
            int emptyTimeseriesCount = 0;

            for ( NwmForecast forecast : document.getForecasts() )
            {
                for ( NwmFeature nwmFeature : forecast.getFeatures() )
                {
                    List<IngestResult> results = this.ingestTimeSeries( forecast,
                                                                        nwmFeature,
                                                                        timeScale,
                                                                        variableName,
                                                                        measurementUnit );
                    ingested.addAll( results );

                    if ( ingested.isEmpty() )
                    {
                        emptyTimeseriesCount++;
                    }
                }
            }

            if ( emptyTimeseriesCount > 0 )
            {
                LOGGER.warn( "Skipped {} empty timeseries from {}",
                             emptyTimeseriesCount,
                             innerUri );
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
            String message = "Interrupted while ingesting NWM data from " + innerUri;
            LOGGER.warn( message, ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            throw new IngestException( "Failed to ingest NWM data from "
                                       + innerUri,
                                       ee );
        }
        finally
        {
            this.shutdownNow();
        }

        return Collections.unmodifiableList( ingested );
    }

    /**
     * Ingests a WRDS NWM time-series.
     * @param forecast the forecast
     * @param nwmFeature the NWM feature
     * @param timeScale the time scale
     * @param variableName the variable name
     * @param measurementUnit the measurement unit name
     * @return the ingest results
     * @throws InterruptedException if interrupted while obtaining ingest results
     * @throws PreIngestException if there were no members available to ingest
     * @throws ExecutionException if ingest failed for any reason
     */

    private List<IngestResult> ingestTimeSeries( NwmForecast forecast,
                                                 NwmFeature nwmFeature,
                                                 TimeScaleOuter timeScale,
                                                 String variableName,
                                                 String measurementUnit )
            throws InterruptedException, ExecutionException
    {
        Future<List<IngestResult>> futureIngestResult = null;

        // Single-valued
        if ( nwmFeature.getMembers().length == 1 )
        {
            TimeSeries<Double> timeSeries =
                    this.createSingleValuedTimeSeries( forecast.getReferenceDatetime(),
                                                       nwmFeature,
                                                       timeScale,
                                                       variableName,
                                                       measurementUnit );

            TimeSeriesIngester ingester = this.createTimeSeriesIngester( timeSeries );

            if ( !timeSeries.getEvents().isEmpty() )
            {
                Stream<TimeSeriesTuple> tupleStream = Stream.of( TimeSeriesTuple.ofSingleValued( timeSeries,
                                                                                                 this.dataSource ) );
                futureIngestResult =
                        this.ingestSaverExecutor.submit( () -> ingester.ingest( tupleStream, this.dataSource ) );
            }
        }
        // Ensemble
        else if ( nwmFeature.getMembers().length > 1 )
        {
            TimeSeries<Ensemble> timeSeries =
                    this.createEnsembleTimeSeries( forecast.getReferenceDatetime(),
                                                   nwmFeature,
                                                   timeScale,
                                                   variableName,
                                                   measurementUnit );

            TimeSeriesIngester ingester = this.createTimeSeriesIngester( timeSeries );

            if ( !timeSeries.getEvents().isEmpty() )
            {
                Stream<TimeSeriesTuple> tupleStream = Stream.of( TimeSeriesTuple.ofEnsemble( timeSeries,
                                                                                             this.dataSource ) );
                futureIngestResult =
                        this.ingestSaverExecutor.submit( () -> ingester.ingest( tupleStream, this.dataSource ) );
            }
        }
        // No members
        else
        {
            throw new PreIngestException( NO_MEMBERS_FOUND_IN_WRDS_NWM_DATA );
        }

        List<IngestResult> ingested = new ArrayList<>();

        if ( Objects.nonNull( futureIngestResult ) )
        {
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

        return Collections.unmodifiableList( ingested );
    }
    
    /**
     * Reads the root document from a URI file scheme source.
     * @param uri the uri
     * @return the root document
     */
    private NwmRootDocument readNwmRootDocumentFromFile( URI uri )
    {
        LOGGER.debug( "Reading a WRDS NWM source from a file, {}.", uri );

        Path forecastPath = Paths.get( uri );

        try ( InputStream stream = Files.newInputStream( forecastPath ) )
        {
            NwmRootDocument document = this.getJsonObjectMapper().readValue( stream, NwmRootDocument.class );
            LOGGER.debug( "Parsed this document: {}", document );
            return document;
        }
        catch ( IOException ioe )
        {
            this.shutdownNow();
            throw new PreIngestException( "Failed to read NWM data from "
                                          + uri,
                                          ioe );
        }
    }
    
    /**
     * Reads the root document from a web source.
     * @param uri the uri
     * @return the root document or null if the document could not be read but no exception was thrown
     */
    private NwmRootDocument readNwmRootDocumentFromWebSource( URI uri )
    {
        LOGGER.debug( "Reading a WRDS NWM source from a web source, {}.", uri );

        try ( WebClient.ClientResponse response = WEB_CLIENT.getFromWeb( uri ) )
        {
            if ( response.getStatusCode() >= 400 && response.getStatusCode() < 500 )
            {
                LOGGER.warn( "Treating HTTP response code {} as no data found from URI {}",
                             response.getStatusCode(),
                             uri );

                String possibleError = this.tryToReadErrorMessage( response );

                if ( possibleError != null )
                {
                    LOGGER.warn( "Found this WRDS error message from URI {}: {}",
                                 uri,
                                 possibleError );
                }

                return null;
            }

            NwmRootDocument document = this.getJsonObjectMapper()
                                           .readValue( response.getResponse(), NwmRootDocument.class );
            LOGGER.debug( "Parsed this document: {}", document );
            return document;
        }
        catch ( IOException ioe )
        {
            this.shutdownNow();
            throw new PreIngestException( "Failed to read NWM data from "
                                          + uri,
                                          ioe );
        }
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

    private TimeSeries<Double> createSingleValuedTimeSeries( Instant referenceDatetime,
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

        if ( members.length == 0 )
        {
            throw new PreIngestException( NO_MEMBERS_FOUND_IN_WRDS_NWM_DATA );
        }
        // Infer that these are single-valued data.
        SortedSet<Event<Double>> events = new TreeSet<>();

        for ( NwmDataPoint dataPoint : members[0].getDataPoints() )
        {
            if ( Objects.isNull( dataPoint ) )
            {
                LOGGER.debug( "Found null datapoint in sole trace at referenceDatetime={} for nwm feature={}",
                              referenceDatetime,
                              rawLocationId );
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

        Geometry geometry = MessageFactory.getGeometry(
                                                        Integer.toString( rawLocationId ) );
        FeatureKey feature = FeatureKey.of( geometry );
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(
                                                             Map.of( referenceTimeType, referenceDatetime ),
                                                             timeScale,
                                                             variableName,
                                                             feature,
                                                             measurementUnit );
        return new Builder<Double>().addEvents( Collections.unmodifiableSortedSet( events ) )
                                    .setMetadata( metadata )
                                    .build();
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

    private TimeSeries<Ensemble> createEnsembleTimeSeries( Instant referenceDatetime,
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

        if ( members.length == 0 )
        {
            throw new PreIngestException( NO_MEMBERS_FOUND_IN_WRDS_NWM_DATA );
        }

        // Infer that this is ensemble data.
        SortedMap<Instant, double[]> primitiveData = new TreeMap<>();

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
                                  i,
                                  referenceDatetime,
                                  rawLocationId );
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
                             i,
                             referenceDatetime,
                             rawLocationId );
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
        Geometry geometry = MessageFactory.getGeometry(
                                                        Integer.toString( rawLocationId ) );
        FeatureKey feature = FeatureKey.of( geometry );
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(
                                                             Map.of( ReferenceTimeType.T0, referenceDatetime ),
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

        return builder.build();
    }

    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @return a TimeSeriesIngester
     */

    TimeSeriesIngester createTimeSeriesIngester( TimeSeries<?> timeSeries )
    {
        if( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Captured time-series {}.", timeSeries );
        }
        
        return this.getTimeSeriesIngester();
    }

    /**
     * Attempt to read an error message from a document like this:
     * {
     *   "error": "API Currently only supports querying by the following: ('nwm_feature_id', 'nws_lid', 'usgs_site_code', 'state', 'rfc', 'huc', 'county', 'tag')"
     * }
     *
     * If anything goes wrong, return null.
     *
     * @param response The response containing a potential error message.
     * @return The value from the above map, null if not found.
     */

    private String tryToReadErrorMessage( WebClient.ClientResponse response )
    {
        try
        {
            NwmRootDocumentWithError document = this.getJsonObjectMapper()
                                                    .readValue( response.getResponse(),
                                                                NwmRootDocumentWithError.class );
            Map<String,String> messages = document.getMessages();

            if ( messages != null )
            {
                return messages.get( "error" );
            }
        }
        catch ( IOException ioe )
        {
            LOGGER.debug( "Failed to parse an error response body {}",
                          response, ioe );
        }

        return null;
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

        if ( WrdsNwmReader.getTrackTimings() && LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "{}", WEB_CLIENT.getTimingInformation() );
        }
    }
}
