 package wres.io.reading;

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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.DayOfWeek.SUNDAY;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;
import static java.time.temporal.TemporalAdjusters.firstDayOfNextYear;
import static java.time.temporal.TemporalAdjusters.next;
import static java.time.temporal.TemporalAdjusters.previousOrSame;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.config.generated.UrlParameter;
import wres.io.concurrency.IngestSaver;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.TimeScales;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;

/**
 * Takes a single web source and splits it into week-long chunks, creates an
 * ingest task for each chunk, GETs chunks of data several at a time, ingests
 * them, and returns the results.
 *
 * One-time use:
 * On construction, creates internal executors.
 * On first call, shuts down its internal executor.
 */
class WebSource implements Callable<List<IngestResult>>
{
    private static final String COULD_NOT_CREATE_URI_FROM = "Could not create URI from ";

    private static final String AND = " and ";

    private static final String SLASH = "/";

    private static final Logger LOGGER = LoggerFactory.getLogger( WebSource.class );

    private static final String ISSUED_DATES_ERROR_MESSAGE =
            "One must specify issued dates with both earliest and latest (e.g. "
            + "<issuedDates earliest=\"2018-12-28T15:42:00Z\" "
            + "latest=\"2019-01-01T00:00:00Z\" />) when using a web API as a "
            + "source for forecasts.";
    
    private static final String VALID_DATES_ERROR_MESSAGE =
            "One must specify dates with both "
            + "earliest and latest (e.g. "
            + "<dates earliest=\"2019-08-10T14:30:00Z\" "
            + "latest=\"2019-08-15T18:00:00Z\" />) "
            + "when using a web API as a source for observations.";

    private static final String DEFAULT_WRDS_PROJ = "UNKNOWN_PROJECT_USING_WRES";


    private final SystemSettings systemSettings;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final TimeScales timeScalesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final ProjectConfig projectConfig;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final URI baseUri;
    private final OffsetDateTime now;

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingResults;

    static WebSource of( SystemSettings systemSettings,
                         Database database,
                         DataSources dataSourcesCache,
                         Features featuresCache,
                         TimeScales timeScalesCache,
                         Ensembles ensemblesCache,
                         MeasurementUnits measurementUnitsCache,
                         ProjectConfig projectConfig,
                         DataSource dataSource,
                         DatabaseLockManager lockManager )
    {
        return new WebSource( systemSettings,
                              database,
                              dataSourcesCache,
                              featuresCache,
                              timeScalesCache,
                              ensemblesCache,
                              measurementUnitsCache,
                              projectConfig,
                              dataSource,
                              lockManager,
                              OffsetDateTime.now() );
    }

    WebSource( SystemSettings systemSettings,
               Database database,
               DataSources dataSourcesCache,
               Features featuresCache,
               TimeScales timeScalesCache,
               Ensembles ensemblesCache,
               MeasurementUnits measurementUnitsCache,
               ProjectConfig projectConfig,
               DataSource dataSource,
               DatabaseLockManager lockManager,
               OffsetDateTime now )
    {
        this.systemSettings = systemSettings;
        this.database = database;
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.timeScalesCache = timeScalesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
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

        // Could be an NPE, but the data source is not null and the nullity of the variable is an effect, not a cause
        if ( Objects.isNull( this.dataSource.getVariable() ) )
        {
            LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( this.projectConfig,
                                                                               this.dataSource.getContext() );

            throw new IllegalArgumentException( "A variable must be declared for a web source but no "
                                                + "variable was found for the "
                                                + lrb
                                                + " web source with URI: "
                                                + this.dataSource.getUri()
                                                + ". Please declare a variable for all "
                                                + lrb
                                                + " web sources." );
        }
        
        ThreadFactory webClientFactory = new BasicThreadFactory.Builder()
                .namingPattern( "WebSource Ingest" )
                .build();

        int concurrentCount = systemSettings.getMaximumWebClientThreads();

        // Because we use a latch and queue below, no need to make this queue
        // any larger than that queue and latch.
        BlockingQueue<Runnable> webClientQueue = new ArrayBlockingQueue<>( concurrentCount );
        this.ingestSaverExecutor = new ThreadPoolExecutor( concurrentCount,
                                                           concurrentCount,
                                                           systemSettings.poolObjectLifespan(),
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
        this.validateSource( this.getDataSource() );
        LOGGER.debug( "Created WebSource for {}", this.dataSource );
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private DataSources getDataSourcesCache()
    {
        return this.dataSourcesCache;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private TimeScales getTimeScalesCache()
    {
        return this.timeScalesCache;
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
    public List<IngestResult> call()
    {
        List<IngestResult> ingestResults = new ArrayList<>();
        Set<String> features =
                ConfigHelper.getFeatureNamesForSource( this.getProjectConfig(),
                                                       this.getDataSourceConfig() );

        Set<Pair<Instant, Instant>> dateRanges = createRanges( this.getProjectConfig(),
                                                               this.getDataSource(),
                                                               this.getNow() );
        Set<URI> alreadySubmittedUris = new HashSet<>();
        DataSource source = this.getDataSource();
        DataSource.DataDisposition disposition;

        if ( this.isUsgsSource( this.getDataSource() ) )
        {
            disposition = DataSource.DataDisposition.JSON_WATERML;
        }
        else if ( this.isWrdsAhpsSource( this.getDataSource() ) || this.isWrdsObsSource( this.getDataSource() ) )
        {
            disposition = DataSource.DataDisposition.JSON_WRDS_AHPS;
        }
        else if ( this.isWrdsNwmSource( this.getDataSource() ) )
        {
            disposition = DataSource.DataDisposition.JSON_WRDS_AHPS;
        }
        else
        {
            throw new UnsupportedOperationException( "Unable to support source "
                                                     + source );
        }

        if ( this.usesFeatureBlocks() )
        {
            // getFeatureStrings replaces shouldCreateUri by filtering nulls.
            List<String> featureNames = new ArrayList<>( features );
            List<String[]> featureBlocks =
                    this.createFeatureBlocks( featureNames );
            try
            {
                for ( String[] featureBlock : featureBlocks )
                {
                    for ( Pair<Instant, Instant> range : dateRanges )
                    {
                        URI uri = createUri( this.getBaseUri(),
                                             this.getDataSource(),
                                             range,
                                             featureBlock );

                        DataSource dSource =
                                DataSource.of( disposition,
                                               this.getSourceConfig(),
                                               this.getDataSourceConfig(),
                                               // Pass through the links because we
                                               // trust the SourceLoader to have
                                               // deduplicated this source if it was
                                               // repeated in other contexts.
                                               this.getDataSource()
                                                   .getLinks(),
                                               uri );
                        LOGGER.debug( "Created datasource {}", dSource );

                        IngestSaver ingestSaver =
                                IngestSaver.createTask()
                                           .withSystemSettings( this.getSystemSettings() )
                                           .withDatabase( this.getDatabase() )
                                           .withDataSourcesCache( this.getDataSourcesCache() )
                                           .withFeaturesCache( this.getFeaturesCache() )
                                           .withTimeScalesCache( this.getTimeScalesCache() )
                                           .withEnsemblesCache( this.getEnsemblesCache() )
                                           .withMeasurementUnitsCache( this.getMeasurementUnitsCache() )
                                           .withDataSource( dSource )
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
                LOGGER.warn( "Interrupted while getting web ingest results.",
                             ie );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException ee )
            {
                throw new IngestException( "Failed to get web ingest results.",
                                           ee );
            }
            finally
            {
                this.shutdownNow();
            }

            return Collections.unmodifiableList( ingestResults );
        }

        try
        {
            for ( String featureName : features )
            {
                for ( Pair<Instant, Instant> range : dateRanges )
                {
                    URI uri = createUri( this.getBaseUri(),
                                         this.getDataSource(),
                                         range,
                                         featureName );

                    if ( alreadySubmittedUris.contains( uri ) )
                    {
                        LOGGER.debug( "Already submitted uri {}, not re-submitting it.",
                                      uri );
                        continue;
                    }

                    DataSource dSource =
                            DataSource.of( disposition,
                                           this.getSourceConfig(),
                                           this.getDataSourceConfig(),
                                           // Pass through the links because we
                                           // trust the SourceLoader to have
                                           // deduplicated this source if it was
                                           // repeated in other contexts.
                                           this.getDataSource()
                                               .getLinks(),
                                           uri );
                    LOGGER.debug( "Created datasource {}", dSource);

                    IngestSaver ingestSaver =
                            IngestSaver.createTask()
                                       .withSystemSettings( this.getSystemSettings() )
                                       .withDatabase( this.getDatabase() )
                                       .withDataSourcesCache( this.getDataSourcesCache() )
                                       .withFeaturesCache( this.getFeaturesCache() )
                                       .withTimeScalesCache( this.getTimeScalesCache() )
                                       .withEnsemblesCache( this.getEnsemblesCache() )
                                       .withMeasurementUnitsCache( this.getMeasurementUnitsCache() )
                                       .withDataSource( dSource )
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
        finally
        {
            this.shutdownNow();
        }

        return Collections.unmodifiableList( ingestResults );
    }


    /**
     * Log a WARN message when a subset of a medium-range dataset is used.
     *
     * This used to be in WrdsNwmReader but was logged per URL rather than per
     * source.
     * @param source The data source.
     */

    private void validateSource( DataSource source )
    {
        if ( DatasourceType.SINGLE_VALUED_FORECASTS.equals( source.getContext()
                                                                  .getType() )
             && InterfaceShortHand.WRDS_NWM.equals( source.getSource()
                                                          .getInterface() )
             && source.getUri()
                      .toString()
                      .toLowerCase()
                      .contains( "medium_range" ) )
        {
            LOGGER.warn( "{}{}{}{}",
                         "Evaluating only the deterministic medium range ",
                         "forecast because 'single valued forecasts' was ",
                         "declared. To evaluate the ensemble forecast, ",
                         "declare 'ensemble forecasts'." );
        }
    }

    private boolean isWrdsNwmSource( DataSource source )
    {
        URI uri = source.getUri();
        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM );
        }

        // Fallback for unspecified interface.
        return  uri.getPath()
                   .toLowerCase()
                   .contains( "nwm" );
    }

    private boolean isWrdsObsSource( DataSource source)
    {
        InterfaceShortHand  interfaceShortHand = source.getSource()
                                                       .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand.equals( InterfaceShortHand.WRDS_OBS );
        }
        return false;
    }

    private boolean isWrdsAhpsSource( DataSource source )
    {
        URI uri = source.getUri();
        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand.equals( InterfaceShortHand.WRDS_AHPS );
        }

        // Fallback for unspecified interface.
        return uri.getPath()
                  .toLowerCase()
                  .endsWith( "ahps" ) ||
               uri.getPath()
                  .toLowerCase()
                  .endsWith( "ahps/" );
    }

    private boolean isUsgsSource( DataSource source )
    {
        URI uri = source.getUri();
        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();
        if ( Objects.nonNull( interfaceShortHand ) )
        {
            return interfaceShortHand.equals( InterfaceShortHand.USGS_NWIS );
        }

        // Fallback for unspecified interface.
        return uri.getHost()
                  .toLowerCase()
                  .contains( "usgs.gov" )
               || uri.getPath()
                     .toLowerCase()
                     .contains( "nwis" );
    }

    /**
     * If this source needs features to be recomposed into blocks of features.
     * @return true if recomposition needed.
     */
    private boolean usesFeatureBlocks()
    {
        return this.isWrdsNwmSource( this.getDataSource() );
    }


    private List<String[]> createFeatureBlocks( List<String> features )
    {
        // Chunk reads by FEATURE_READ_COUNT
        final int FEATURE_READ_COUNT;
        if ( this.isWrdsNwmSource( this.getDataSource() ) )
        {
            FEATURE_READ_COUNT = 25;
        }
        else if ( this.isUsgsSource( this.getDataSource() ) )
        {
            FEATURE_READ_COUNT = 10;
        }
        else
        {
            throw new UnsupportedOperationException( "Method only supports WRDS NWM or USGS for now." );
        }

        int maxCountOfBlocks = ( features.size()
                                 / FEATURE_READ_COUNT )
                               + 1;
        List<String[]> featureBlocks = new ArrayList<>( maxCountOfBlocks );

        int j = 0;
        String[] block;

        // The last block is unlikely to be exactly FEATURE_READ_COUNT
        int remaining = features.size();

        if ( remaining < FEATURE_READ_COUNT )
        {
            block = new String[remaining];
        }
        else
        {
            block = new String[FEATURE_READ_COUNT];
        }

        for ( int i = 0; i < features.size(); i++ )
        {
            if ( i % FEATURE_READ_COUNT == 0 )
            {
                LOGGER.debug( "Found we are at a boundary. i={}, j={}", i, j );
                // After the first block is written, add to list.
                if ( i > 0 )
                {
                    featureBlocks.add( block );
                }

                // The last block is unlikely to be exactly FEATURE_READ_COUNT
                remaining = features.size() - i;

                if ( remaining < FEATURE_READ_COUNT )
                {
                    LOGGER.debug( "Creating last int[] of size {}", remaining );
                    block = new String[remaining];
                }
                else
                {
                    LOGGER.debug( "Creating full sized int[] of size {}",
                                  FEATURE_READ_COUNT );
                    block = new String[FEATURE_READ_COUNT];
                }

                j = 0;
            }

            block[j] = features.get( i );
            j++;
        }

        // Add the last feature block
        featureBlocks.add( block );

        return Collections.unmodifiableList( featureBlocks );
    }


    /**
     * Returns a set of datetime ranges based on the kind of DataSource given.
     *
     * Calls the appropriate method below based on the DataSource.
     *
     * For NWM data, for example, the ranges may be weekly, whereas for USGS
     * NWIS data the ranges may be annual.
     *
     * @param declaration The project declaration.
     * @param dataSource The to-be-ingested data source.
     * @param nowDate The date "now", to avoid putting future date into ranges.
     * @return A set of datetime ranges.
     */

    private Set<Pair<Instant,Instant>> createRanges( ProjectConfig declaration,
                                                     DataSource dataSource,
                                                     OffsetDateTime nowDate )
    {
        if ( this.isUsgsSource( dataSource ) )
        {
            return this.createYearRanges( declaration,
                                          dataSource,
                                          nowDate );
        }
        else if ( this.isWrdsObsSource( dataSource ) )
        {
            return this.createYearRanges( declaration,
                                          dataSource,
                                          nowDate );
        }
        else if ( this.isWrdsNwmSource( dataSource ) )
        {
            return this.createWeekRanges( declaration,
                                          dataSource,
                                          nowDate );
        }
        else if ( this.isWrdsAhpsSource( dataSource ) )
        {
            return this.createWrdsAhpsRanges( declaration,
                                              dataSource );
        }
        else
        {
            throw new UnsupportedOperationException( "Could not create ranges for "
                                                     + dataSource );
        }
    }

    private Set<Pair<Instant,Instant>> createWrdsAhpsRanges( ProjectConfig declaration,
                                                             DataSource dataSource )
    {
        return this.createSimpleRange( declaration, dataSource );
    }

    /**
     * Returns the exact forecast range from the declaration instead of breaking
     * it apart.
     * @param declaration
     * @param dataSource
     * @return
     */
    private Set<Pair<Instant,Instant>> createSimpleRange( ProjectConfig declaration,
                                                          DataSource dataSource )
    {

        Objects.requireNonNull( declaration );
        Objects.requireNonNull( declaration.getPair() );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        boolean isForecast = ConfigHelper.isForecast( dataSource.getContext() );

        if ( ( isForecast && declaration.getPair().getIssuedDates() == null ) )
        {
            throw new ProjectConfigException( declaration.getPair(),
                                              ISSUED_DATES_ERROR_MESSAGE );
        }
        else if( !isForecast && declaration.getPair().getDates() == null )
        {
            throw new ProjectConfigException( declaration.getPair(),
                                              VALID_DATES_ERROR_MESSAGE );
        }
        
        DateCondition dates = declaration.getPair()
                                         .getDates();

        if ( isForecast )
        {
            dates = declaration.getPair()
                               .getIssuedDates();
        }

        if ( dates.getEarliest() == null
             || dates.getLatest() == null )
        {
            throw new ProjectConfigException( dates,
                                              VALID_DATES_ERROR_MESSAGE );
        }

        String specifiedEarliest = dates.getEarliest();
        Instant earliest = Instant.parse( specifiedEarliest );

        String specifiedLatest = dates.getLatest();
        Instant latest = Instant.parse( specifiedLatest );
        Pair<Instant,Instant> range = Pair.of( earliest, latest );
        return Set.of( range );
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

        if ( ( isForecast && config.getPair().getIssuedDates() == null ) )
        {
            throw new ProjectConfigException( config.getPair(),
                                              ISSUED_DATES_ERROR_MESSAGE );
        }
        else if( !isForecast && config.getPair().getDates() == null )
        {
            throw new ProjectConfigException( config.getPair(),
                                              VALID_DATES_ERROR_MESSAGE );
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
                                              VALID_DATES_ERROR_MESSAGE );
        }

        SortedSet<Pair<Instant,Instant>> weekRanges = new TreeSet<>();

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
     * Similar to above, but deeper-in-duration: a year.
     */
    private Set<Pair<Instant,Instant>> createYearRanges( ProjectConfig config,
                                                         DataSource dataSource,
                                                         OffsetDateTime nowDate )
    {
        Objects.requireNonNull( config );
        Objects.requireNonNull( config.getPair() );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( dataSource.getContext() );

        boolean isForecast = ConfigHelper.isForecast( dataSource.getContext() );

        if ( ( isForecast && config.getPair().getIssuedDates() == null ) )
        {
            throw new ProjectConfigException( config.getPair(),
                                              ISSUED_DATES_ERROR_MESSAGE );
        }
        else if( !isForecast && config.getPair().getDates() == null )
        {
            throw new ProjectConfigException( config.getPair(),
                                              VALID_DATES_ERROR_MESSAGE );
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
                                              VALID_DATES_ERROR_MESSAGE );
        }

        SortedSet<Pair<Instant,Instant>> yearRanges = new TreeSet<>();

        OffsetDateTime earliest;
        String specifiedEarliest = dates.getEarliest();

        OffsetDateTime latest;
        String specifiedLatest = dates.getLatest();

        earliest = OffsetDateTime.parse( specifiedEarliest )
                                 .with( firstDayOfYear() )
                                 .withHour( 0 )
                                 .withMinute( 0 )
                                 .withSecond( 0 )
                                 .withNano( 0 );

        LOGGER.debug( "createYearRanges(): given {} calculated {} for earliest.",
                      specifiedEarliest, earliest );

        // Intentionally keep this raw, un-next-year-ified.
        latest = OffsetDateTime.parse( specifiedLatest );

        LOGGER.debug( "createYearRanges(): given {} parsed {} for latest.",
                      specifiedLatest, latest );

        OffsetDateTime left = earliest;
        OffsetDateTime right = left.with( firstDayOfNextYear() );

        while ( left.isBefore( latest ) )
        {
            // Because we chunk a year at a time, and because these will not
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
            LOGGER.debug( "createYearRanges(): created range {}", range );
            yearRanges.add( range );
            left = left.with( firstDayOfNextYear() );
            right = right.with( firstDayOfNextYear() );
        }

        LOGGER.debug( "createYearRanges(): calculated ranges {}", yearRanges );

        return Collections.unmodifiableSet( yearRanges );
    }


    /**
     * Create a uri for a single location. Older method than below createUri.
     * Used for USGS NWIS and WRDS AHPS services as of 2020-12-30.
     * Supports both USGS NWIS and WRDS AHPS services.
     * @param baseUri The uri prefix.
     * @param dataSource The data source.
     * @param range The date range.
     * @param featureName The individual feature.
     * @return The URI to use to get data.
     */

    URI createUri( URI baseUri,
                   DataSource dataSource,
                   Pair<Instant,Instant> range,
                   String featureName )
    {
        if ( this.isWrdsAhpsSource( dataSource ) )
        {
            return this.createWrdsAhpsUri( baseUri,
                                           range,
                                           featureName,
                                           dataSource.getContext()
                                                     .getUrlParameter(),
                                           false );
        }
        else if ( this.isWrdsObsSource( dataSource ) )
        {
            return this.createWrdsAhpsUri( baseUri,
                                           range,
                                           featureName,
                                           dataSource.getContext()
                                                     .getUrlParameter(),
                                           true );
        }
        else if ( this.isUsgsSource( dataSource ) )
        {
            return this.createUsgsNwisUri( baseUri,
                                           dataSource,
                                           range,
                                           new String[] { featureName } );
        }
        else
        {
            // #74994-8
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While attempting to create a precise URI from a "
                              + "base URI, failed to recognize the base URI '{}'"
                              + "as a standard type. Returning the "
                              + " base URI instead.",
                              baseUri );
            }

            return baseUri;
        }
    }


    /**
     * Create a URI for multiple locations. Newer method than above createUri.
     * Only used for WRDS NWM services as of 2020-12-30.
     * Supports both USGS NWIS and WRDS NWM services.
     * @param baseUri The URI prefix.
     * @param dataSource The data source.
     * @param range The date range.
     * @param featureNames The group of features to include in single URI.
     * @return The URI to use to get data.
     */

    URI createUri( URI baseUri,
                   DataSource dataSource,
                   Pair<Instant,Instant> range,
                   String[] featureNames )
    {
        if ( this.isWrdsNwmSource( dataSource ) )
        {
            return this.createWrdsNwmUri( baseUri, dataSource, range, featureNames );
        }
        else if ( this.isUsgsSource( dataSource ) )
        {
            return this.createUsgsNwisUri( baseUri, dataSource, range, featureNames );
        }
        else
        {
            throw new ProjectConfigException( dataSource.getContext(),
                                              "Unsupported URI base "
                                              + baseUri +
                                              " for four-param createUri method." );
        }
    }

    /**
     * Specific to USGS API, get a URI for a given issued date range and feature
     *
     * <p>Expecting a USGS URI like this:
     * https://nwis.waterservices.usgs.gov/nwis/iv/</p>
     * @param baseUri The base uri associated with this source
     * @param dataSource The data source for which to create a URI.
     * @param range the range of dates (from left to right)
     * @param featureNames The features to include in the request URI.
     * @return a URI suitable to get the data from WRDS API
     */

    private URI createUsgsNwisUri( URI baseUri,
                                   DataSource dataSource,
                                   Pair<Instant,Instant> range,
                                   String[] featureNames )
    {
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( range );
        Objects.requireNonNull( featureNames );
        Objects.requireNonNull( range.getLeft() );
        Objects.requireNonNull( range.getRight() );

        // example "?format=json&sites=09165000&parameterCd=00060&startDT=2018-10-01T00:00:0
        if ( !baseUri.getHost()
                     .toLowerCase()
                     .contains( "usgs.gov" )
             && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Expected URI like '"
                         + "https://nwis.waterservices.usgs.gov/nwis/iv'"
                         + " but instead got {}.", baseUri.toString() );
        }

        Map<String, String> urlParameters = createUsgsUrlParameters( range,
                                                                     featureNames,
                                                                     dataSource );
        return getURIWithParameters( baseUri,
                                     urlParameters );
    }


    /**
     * Specific to WRDS AHPS API, get URI for given issued date range and feature
     *
     * <p>Expecting a wrds URI like this:
     * http://redacted/api/v1/forecasts/streamflow/ahps</p>
     * @param range the range of dates (from left to right)
     * @param nwsLocationId The feature for which to get data.
     * @param observed True for observations, false for AHPS forecasts.
     * @return a URI suitable to get the data from WRDS API
     */

    private URI createWrdsAhpsUri( URI baseUri,
                                   Pair<Instant,Instant> range,
                                   String nwsLocationId,
                                   List<UrlParameter> additionalParameters,
                                   boolean observed )
    {

        String basePath = baseUri.getPath();

        // Tolerate either a slash at end or not.
        if ( !basePath.endsWith( SLASH ) )
        {
            basePath = basePath + SLASH;
        }
        
        //Add nws_lid to the end of the path.
        //TODO Remove the outer if-check once the old, 1.1 API is gone. 
        if (!basePath.contains( "v1.1" ) && !basePath.endsWith("nws_lid/"))
        {
            basePath = basePath + "nws_lid/";
        }

        Map<String, String> wrdsParameters = createWrdsAhpsUrlParameters( range,
                                                                          additionalParameters,
                                                                          observed );
        String pathWithLocation = basePath 
                                  + nwsLocationId;
        URIBuilder uriBuilder = new URIBuilder( this.getBaseUri() );
        uriBuilder.setPath( pathWithLocation );

        URI uriWithLocation;
        try
        {
            uriWithLocation = uriBuilder.build();
        }
        catch ( URISyntaxException use )
        {
            throw new IllegalArgumentException( COULD_NOT_CREATE_URI_FROM
                                                + this.getBaseUri().toString()
                                                + AND
                                                + pathWithLocation, use );
        }

        return getURIWithParameters( uriWithLocation,
                                     wrdsParameters );
    }


    /**
     * Specific to WRDS NWM API, create a URI for nwm forecasts with given range
     *
     * Assumes validation of the combination of arguments happened earlier.
     *
     * @param baseUri Base URI, a URI with missing parameters to be added.
     * @param dataSource The data source information, because variable needed.
     * @param issuedRange The datetime range of nwm reference datetimes to set.
     * @param featureNames The List of NWM features used in this evaluation.
     * @return A URI with full path and parameters to get NWM from WRDS NWM API.
     * @throws NullPointerException When any argument is null.
     */

    private URI createWrdsNwmUri( URI baseUri,
                                  DataSource dataSource,
                                  Pair<Instant, Instant> issuedRange,
                                  String[] featureNames )
    {
        Objects.requireNonNull( baseUri );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( issuedRange );
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
                                       .getType()
                                       .equals( DatasourceType.ENSEMBLE_FORECASTS );

        Map<String, String> wrdsParameters = createWrdsAhpsNwmParameters( issuedRange,
                                                                          isEnsemble,
                                                                          dataSource.getContext()
                                                                                    .getUrlParameter() );
        StringJoiner joiner = new StringJoiner( "," );

        for ( String featureName : featureNames )
        {
            joiner.add( featureName );
        }

        String featureNamesCsv = joiner.toString();
        String pathWithLocation = basePath + variableName + "/nwm_feature_id/"
                                  + featureNamesCsv + SLASH;

        if ( pathWithLocation.length() > 1960 )
        {
            LOGGER.warn( "Built an unexpectedly long path: {}",
                         pathWithLocation );
        }

        URIBuilder uriBuilder = new URIBuilder( this.getBaseUri() );
        uriBuilder.setPath( pathWithLocation );

        URI uriWithLocation;
        try
        {
            uriWithLocation = uriBuilder.build();
        }
        catch ( URISyntaxException use )
        {
            throw new IllegalArgumentException( COULD_NOT_CREATE_URI_FROM
                                                + this.getBaseUri().toString()
                                                + AND
                                                + pathWithLocation, use );
        }

        return getURIWithParameters( uriWithLocation,
                                     wrdsParameters );
    }

    /**
     * Specific to USGS NWIS API, get date range url parameters
     * @param range the date range to set parameters for
     * @param siteCodes The USGS site codes desired.
     * @param dataSource The data source from which this request came.
     * @return the key/value parameters
     * @throws NullPointerException When arg or value enclosed inside arg is null
     */

    private Map<String,String> createUsgsUrlParameters( Pair<Instant,Instant> range,
                                                        String[] siteCodes,
                                                        DataSource dataSource )
    {
        LOGGER.trace( "Called createUsgsUrlParameters with {}, {}, {}",
                      range, siteCodes, dataSource );
        Objects.requireNonNull( range );
        Objects.requireNonNull( siteCodes );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( range.getLeft() );
        Objects.requireNonNull( range.getRight() );
        Objects.requireNonNull( dataSource.getVariable() );
        Objects.requireNonNull( dataSource.getVariable().getValue() );

        StringJoiner siteJoiner = new StringJoiner( "," );

        for ( String site: siteCodes )
        {
            siteJoiner.add( site );
        }

        // The start datetime needs to be one second later than the next
        // week-range's end datetime or we end up with duplicates.
        // This follows the "pools are inclusive/exclusive" convention of WRES.
        // For some reason, 1 to 999 milliseconds are not enough.
        Instant startDateTime = range.getLeft()
                                     .plusSeconds( 1 );
        Map<String,String> urlParameters = new HashMap<>( 5 );

        // Caller-supplied additional parameters are lower precedence, put first
        for ( UrlParameter parameter : dataSource.getContext()
                                                 .getUrlParameter() )
        {
            urlParameters.put( parameter.getName(), parameter.getValue() );
        }

        urlParameters.put( "format", "json" );
        urlParameters.put( "parameterCd", dataSource.getVariable()
                                                    .getValue() );
        urlParameters.put( "startDT", startDateTime.toString() );
        urlParameters.put( "endDT", range.getRight().toString() );
        urlParameters.put( "sites", siteJoiner.toString() );

        return Collections.unmodifiableMap( urlParameters );
    }


    /**
     * Specific to WRDS API, get date range url parameters
     * @param issuedRange the date range to set parameters for
     * @return the key/value parameters
     */

    private Map<String,String> createWrdsAhpsUrlParameters( Pair<Instant,Instant> issuedRange,
                                                            List<UrlParameter> additionalParameters, 
                                                            boolean observed)
    {
        Map<String,String> urlParameters = new HashMap<>( 2 );

        //Set the proj field, allowing for a user to override it with a URL 
        //parameter, which is handled next.
        urlParameters.put( "proj", DEFAULT_WRDS_PROJ );

        // Caller-supplied additional parameters are lower precedence, put first
        for ( UrlParameter parameter : additionalParameters )
        {
            urlParameters.put( parameter.getName(), parameter.getValue() );
        }

        String timeTag = "issuedTime";
        if ( observed )
        {
            timeTag = "validTime";
        }

        urlParameters.put( timeTag, "[" + issuedRange.getLeft().toString()
                                    + "," + issuedRange.getRight().toString()
                                    + "]" );

        return Collections.unmodifiableMap( urlParameters );
    }


    /**
     * Specific to WRDS API, get date range url parameters
     * @param issuedRange the date range to set parameters for
     * @return the key/value parameters
     */

    private Map<String,String> createWrdsAhpsNwmParameters( Pair<Instant,Instant> issuedRange,
                                                            boolean isEnsemble,
                                                            List<UrlParameter> additionalParameters )
    {
        Map<String,String> urlParameters = new HashMap<>( 3 );

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

        //Set the default WRDS proj, but allow a user to override it
        //through URL parameter processed below.
        urlParameters.put( "proj", DEFAULT_WRDS_PROJ );

        // Caller-supplied additional parameters are lower precedence, put first
        // This will override the parameter added above.
        for ( UrlParameter parameter : additionalParameters )
        {
            urlParameters.put( parameter.getName(), parameter.getValue() );
        }

        String leftWrdsFormattedDate = iso8601TruncatedToHourFromInstant( issuedRange.getLeft() );
        String rightWrdsFormattedDate = iso8601TruncatedToHourFromInstant( issuedRange.getRight() );
        urlParameters.put( "reference_time", "(" + leftWrdsFormattedDate
                                         + "," + rightWrdsFormattedDate
                                         + "]" );
        urlParameters.put( "validTime", "all" );

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
            throw new IllegalArgumentException( COULD_NOT_CREATE_URI_FROM
                                                + uri.toString() + AND
                                                + urlParameters.toString(), e );
        }
    }


    void shutdownNow()
    {
        List<Runnable> abandoned = this.ingestSaverExecutor.shutdownNow();

        if ( !abandoned.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Abandoned ingest of {} URIs associated with source {}",
                         abandoned.size(), this.getDataSource() );
        }
    }
}
