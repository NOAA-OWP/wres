package wres.io.reading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.io.reading.DataSource.DataDisposition.COMPLEX;
import static wres.io.reading.DataSource.DataDisposition.FILE_OR_DIRECTORY;
import static wres.io.reading.DataSource.DataDisposition.GZIP;
import static wres.io.reading.DataSource.DataDisposition.NETCDF_GRIDDED;
import static wres.io.reading.DataSource.DataDisposition.UNKNOWN;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.IngestSaver;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.nwm.NWMReader;
import wres.io.removal.IncompleteIngest;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.util.NetCDF;

/**
 * Evaluates datasources specified within a project configuration and determines
 * what data should be ingested. Asynchronous tasks for each file needed for
 * ingest are created and sent to the Exector for ingestion.
 * @author Christopher Tubbs
 */
public class SourceLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceLoader.class);
    private static final long KEY_NOT_FOUND = Long.MIN_VALUE;

    private final SystemSettings systemSettings;
    private final ExecutorService executor;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;

    /**
     * The project configuration indicating what data to use
     */
    private final ProjectConfig projectConfig;
    private final DatabaseLockManager lockManager;

    private enum SourceStatus
    {
        /** The status of a source not present in the database at all. */
        INCOMPLETE_WITH_NO_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING,
        /** The status of a source begun and in progress. */
        INCOMPLETE_WITH_TASK_CLAIMING_AND_TASK_CURRENTLY_INGESTING,
        /** The status of a source ingested completely. */
        COMPLETED,
        /** The status of a source begun but abandoned by the claiming task. */
        INCOMPLETE_WITH_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING,
        /** The status of a source requiring decomposition, e.g. archive|web. */
        REQUIRES_DECOMPOSITION,
        /** The status of a source not able to be queried (it is invalid). */
        INVALID
    }

    /**
     * @param systemSettings The system settings to use.
     * @param executor The executor to use.
     * @param database The database to use.
     * @param dataSourcesCache The data sources cache to use.
     * @param featuresCache The features cache to use.
     * @param variablesCache The variables cache to use.
     * @param ensemblesCache The ensembles cache to use.
     * @param measurementUnitsCache The measurement units cache to use.
     * @param projectConfig the project configuration
     * @param lockManager the tool to manage ingest locks, shared per ingest
     */
    public SourceLoader( SystemSettings systemSettings,
                         ExecutorService executor,
                         Database database,
                         DataSources dataSourcesCache,
                         Features featuresCache,
                         Variables variablesCache,
                         Ensembles ensemblesCache,
                         MeasurementUnits measurementUnitsCache,
                         ProjectConfig projectConfig,
                         DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( database );
        Objects.requireNonNull( dataSourcesCache );
        Objects.requireNonNull( featuresCache );
        Objects.requireNonNull( variablesCache );
        Objects.requireNonNull( ensemblesCache );
        Objects.requireNonNull( measurementUnitsCache );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( lockManager );
        this.systemSettings = systemSettings;
        this.executor = executor;
        this.database = database;
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.projectConfig = projectConfig;
        this.lockManager = lockManager;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private ExecutorService getExecutor()
    {
        return this.executor;
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

    private Variables getVariablesCache()
    {
        return this.variablesCache;
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

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    /**
     * Ingest data
     * @return List of Future file ingest results
     * @throws IOException when no data is found
     * @throws IngestException when getting project details fails
     */
    public List<CompletableFuture<List<IngestResult>>> load() throws IOException
    {
        LOGGER.info( "Parsing the declared datasets. {}{}{}{}{}{}",
                     "Depending on many factors (including dataset size, ",
                     "dataset design, data service implementation, service ",
                     "availability, network bandwidth, network latency, ",
                     "storage bandwidth, storage latency, concurrent ",
                     "evaluations on shared resources, concurrent computation ",
                     "on shared resources) this can take a while..." );
        List<CompletableFuture<List<IngestResult>>> savingSources = new ArrayList<>();

        // Create the sources for which ingest should be attempted, together with
        // any required links. A link is an additional entry in wres.ProjectSource.
        // A link is required for each context in which the source appears within
        // a project. A context means LeftOrRightOrBaseline.
        Set<DataSource> sources = SourceLoader.createSourcesToLoadAndLink( this.systemSettings,
                                                                           this.projectConfig );

        LOGGER.debug( "Created these sources to load and link: {}", sources );

        // Load each source and create any additional links required
        for( DataSource source : sources )
        {
            savingSources.addAll( this.loadSource( source ) );
        }

        return Collections.unmodifiableList( savingSources );
    }

    /**
     * Attempts to load the input source.
     * 
     * @param source The data source
     * @return A listing of asynchronous tasks dispatched to ingest data
     * @throws FileNotFoundException when a source file is not found
     * @throws IOException when a source file was not readable
     */
    private List<CompletableFuture<List<IngestResult>>> loadSource( DataSource source )
            throws IOException
    {
        // Try to load non-file source
        CompletableFuture<List<IngestResult>> nonFileIngest = loadNonFileSource( source );

        // When the non-file source is detected, short-circuit the file way.
        if ( nonFileIngest != null )
        {
            return Collections.singletonList( nonFileIngest );
        }

        // Proceed with files
        List<CompletableFuture<List<IngestResult>>> savingFiles = new ArrayList<>();

        if ( !source.hasSourcePath() )
        {
            throw new FileNotFoundException( "Found a file data source with an invalid path: "
                                             + source );
        }

        File sourceFile = Paths.get( source.getUri() ).toFile();

        if ( !sourceFile.exists() )
        {
            throw new FileNotFoundException( "The path: '" +
                                             sourceFile.getCanonicalPath()
                                             +
                                             "' was not found." );
        }
        else if ( !sourceFile.canRead() )
        {
            throw new IOException( "The path: '" + sourceFile.getCanonicalPath()
                                   + "' was not readable. Please set "
                                   + "the permissions of that path to "
                                   + "readable for user '"
                                   + System.getProperty( "user.name" )
                                   + "' or run WRES as a user with read"
                                   + " permissions on that path." );
        }
        else if ( sourceFile.isFile() )
        {
            List<CompletableFuture<List<IngestResult>>> futureResults =
                    this.ingestFile( source,
                                     this.getProjectConfig(),
                                     this.getLockManager() );
            savingFiles.addAll( futureResults );
        }
        else
        {
            LOGGER.warn( "'{}' is not a source of valid input data.",
                         sourceFile.getCanonicalPath() );
        }

        return Collections.unmodifiableList( savingFiles );
    }

    /**
     * Load a given source from a given config, return null if file-like source
     * 
     * TODO: create links for a non-file source when it appears in more than 
     * one context, i.e. {@link LeftOrRightOrBaseline}. 
     * See {@link #ingestFile(DataSource, ProjectConfig, DatabaseLockManager)}
     * for how this is done with a file source.
     * 
     * See #67774
     * 
     * @param source the data source
     * @return a single future list of results or null if source was file-like
     */

    private CompletableFuture<List<IngestResult>> loadNonFileSource( DataSource source )
    {
        InterfaceShortHand interfaceShortHand = source.getSource()
                                                      .getInterface();

        if ( interfaceShortHand != null )
        {
            LOGGER.debug( "The data at '{}' will be re-composed because an interface short-hand was specified.",
                          source );
            if ( interfaceShortHand.equals( InterfaceShortHand.WRDS_AHPS )
                 || interfaceShortHand.equals( InterfaceShortHand.USGS_NWIS )
                 || interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM ) )
            {
                WebSource webSource = WebSource.of( this.getSystemSettings(),
                                                    this.getDatabase(),
                                                    this.getDataSourcesCache(),
                                                    this.getFeaturesCache(),
                                                    this.getVariablesCache(),
                                                    this.getEnsemblesCache(),
                                                    this.getMeasurementUnitsCache(),
                                                    this.getProjectConfig(),
                                                    source,
                                                    this.getLockManager() );
                return CompletableFuture.supplyAsync( webSource::call,
                                                      this.getExecutor() );
            }
            else
            {
                // Must be NWM, right?
                NWMReader nwmReader = new NWMReader( this.getSystemSettings(),
                                                     this.getDatabase(),
                                                     this.getFeaturesCache(),
                                                     this.getVariablesCache(),
                                                     this.getEnsemblesCache(),
                                                     this.getMeasurementUnitsCache(),
                                                     this.getProjectConfig(),
                                                     source,
                                                     this.getLockManager() );
                return CompletableFuture.supplyAsync( nwmReader::call,
                                                      this.getExecutor() );
            }
        }

        // See #63493. This method of identification, which is tied to
        // source format, does not work well. The format should not designate
        // whether a source originates from a file or from a service. 
        // Also, there is an absence of consistency in whether a service-like
        // source requires that the URI is declared. For example, at the time
        // of writing, it is required for WRDS, but not for USGS NWIS.
        // As a result, expect some miss-identification of sources as 
        // originating from services vs. files.

        URI sourceUri = source.getSource()
                              .getValue();

        if ( sourceUri != null
             && sourceUri.getScheme() != null
             && sourceUri.getHost() != null )
        {
            WebSource webSource = WebSource.of( this.getSystemSettings(),
                                                this.getDatabase(),
                                                this.getDataSourcesCache(),
                                                this.getFeaturesCache(),
                                                this.getVariablesCache(),
                                                this.getEnsemblesCache(),
                                                this.getMeasurementUnitsCache(),
                                                this.getProjectConfig(),
                                                source,
                                                this.getLockManager() );
            return CompletableFuture.supplyAsync( webSource::call,
                                                  this.getExecutor() );
        }
        else
        {
            // At this point we should have a file, but check first.
            if ( sourceUri == null )
            {
                throw new ProjectConfigException( source.getSource(),
                                                  "Unable to use the source "
                                                  + "because no URI was "
                                                  + "specified." );
            }

            // Null signifies the source was a file-ish source.
            return null;
        }
    }


    /**
     * Ingest data where the hash is known in advance. This is one of the two
     * innermost versions of the ingestData method.
     * @param source the source to ingest
     * @param projectConfig the project configuration causing the ingest
     * @param lockManager the lock manager to use
     * @return a list of future lists of ingest results, possibly empty
     */

    private List<CompletableFuture<List<IngestResult>>> ingestData( DataSource source,
                                                                    ProjectConfig projectConfig,
                                                                    DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( lockManager );

        List<CompletableFuture<List<IngestResult>>> tasks = new ArrayList<>();
        CompletableFuture<List<IngestResult>> task;

        // When the TimeSeries is present, bypass IngestSaver route, use a
        // TimeSeriesIngester instead.
        if ( Objects.nonNull( source.getTimeSeries() ) )
        {
            TimeSeriesIngester ingester =
                    TimeSeriesIngester.of( this.getSystemSettings(),
                                           this.getDatabase(),
                                           this.getFeaturesCache(),
                                           this.getVariablesCache(),
                                           this.getEnsemblesCache(),
                                           this.getMeasurementUnitsCache(),
                                           this.getProjectConfig(),
                                           source,
                                           this.getLockManager(),
                                           source.getTimeSeries() );
            task = CompletableFuture.supplyAsync( ingester::call,
                                                  this.getExecutor() );
        }
        else
        {
            IngestSaver ingestSaver =
                    IngestSaver.createTask()
                               .withSystemSettings( this.getSystemSettings() )
                               .withDatabase( this.getDatabase() )
                               .withDataSourcesCache( this.getDataSourcesCache() )
                               .withFeaturesCache( this.getFeaturesCache() )
                               .withVariablesCache( this.getVariablesCache() )
                               .withEnsemblesCache( this.getEnsemblesCache() )
                               .withMeasurementUnitsCache( this.getMeasurementUnitsCache() )
                               .withProject( projectConfig )
                               .withDataSource( source )
                               .withoutHash()
                               .withProgressMonitoring()
                               .withLockManager( lockManager )
                               .build();
            task = CompletableFuture.supplyAsync( ingestSaver::call,
                                                  this.getExecutor() );
        }

        tasks.add( task );
        return Collections.unmodifiableList( tasks );
    }


    /**
     * Ingest data where the the source is known to be a file.
     *
     * @param source the source to ingest, must be a file
     * @param projectConfig the project configuration causing the ingest
     * @param lockManager the lock manager to use
     * @return a list of future lists of ingest results, possibly empty
     */
    private List<CompletableFuture<List<IngestResult>>> ingestFile( DataSource source,
                                                                    ProjectConfig projectConfig,
                                                                    DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( source );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( lockManager );

        LOGGER.debug( "ingestFile called: {}", source );

        URI sourceUri = source.getUri();
        List<CompletableFuture<List<IngestResult>>> tasks = new ArrayList<>();
        FileEvaluation checkIngest = shouldIngest( source,
                                                   lockManager );
        SourceStatus sourceStatus = checkIngest.getSourceStatus();

        if ( sourceStatus.equals( SourceStatus.REQUIRES_DECOMPOSITION ) )
        {
            // When there is an archive, shouldIngest() will be true however
            // the hash will not yet have been computed, because the inner
            // source identities are what is important, and those inner sources
            // will be hashed later in the process.
            IngestSaver ingestSaver = IngestSaver.createTask()
                                                 .withSystemSettings( this.getSystemSettings() )
                                                 .withDatabase( this.getDatabase() )
                                                 .withDataSourcesCache( this.getDataSourcesCache() )
                                                 .withFeaturesCache( this.getFeaturesCache() )
                                                 .withVariablesCache( this.getVariablesCache() )
                                                 .withEnsemblesCache( this.getEnsemblesCache() )
                                                 .withMeasurementUnitsCache( this.getMeasurementUnitsCache() )
                                                 .withProject( projectConfig )
                                                 .withDataSource( source )
                                                 .withoutHash()
                                                 .withProgressMonitoring()
                                                 .withLockManager( lockManager )
                                                 .build();
            CompletableFuture<List<IngestResult>> future =
                    CompletableFuture.supplyAsync( ingestSaver::call,
                                                   this.getExecutor() );
            tasks.add( future );
        }
        else if ( sourceStatus.equals( SourceStatus.INCOMPLETE_WITH_NO_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING )
                  || sourceStatus.equals( SourceStatus.INCOMPLETE_WITH_TASK_CLAIMING_AND_TASK_CURRENTLY_INGESTING ) )
        {
                List<CompletableFuture<List<IngestResult>>> futureList =
                        this.ingestData( source,
                                         projectConfig,
                                         lockManager );
                tasks.addAll( futureList );
        }
        else if ( sourceStatus.equals( SourceStatus.INCOMPLETE_WITH_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING ) )
        {
            // When the ingest requires retry and also is not in progress,
            // attempt cleanup: some process trying to ingest the source
            // died during ingest and data needs to be cleaned out.
            long surrogateKey = checkIngest.getSurrogateKey();
            LOGGER.info(
                    "Another WRES instance started to ingest a source like '{}' identified by '{}' but did not finish, cleaning up...",
                    sourceUri,
                    surrogateKey );
            IncompleteIngest.removeSourceDataSafely( this.getDatabase(),
                                                     this.getDataSourcesCache(),
                                                     surrogateKey,
                                                     lockManager );
            List<CompletableFuture<List<IngestResult>>> futureList =
                    this.ingestData( source,
                                     projectConfig,
                                     lockManager );
            tasks.addAll( futureList );
        }
        else if ( sourceStatus.equals( SourceStatus.COMPLETED ) )
        {
            LOGGER.debug(
                    "Data will not be loaded from '{}'. That data is already in the database",
                    sourceUri );

            // Fake a future, return result immediately.
            tasks.add( IngestResult.fakeFutureSingleItemListFrom(
                    projectConfig,
                    source,
                    checkIngest.getSurrogateKey(),
                    !checkIngest.ingestMarkedComplete() ) );
        }
        else if ( sourceStatus.equals( SourceStatus.INVALID ) )
        {
            LOGGER.warn( "Data will not be loaded from invalid URI '{}'",
                         sourceUri );
        }
        else
        {
            throw new IllegalStateException( "Unexpected SourceStatus "
                                             + sourceStatus
                                             + " for "
                                             + source );
        }

        LOGGER.trace( "ingestData returning tasks {} for URI {}",
                      tasks,
                      sourceUri );

        return Collections.unmodifiableList( tasks );
    }


    /**
     * Determines whether or not data at an indicated path should be ingested.
     * archived data will always be further evaluated to determine whether its
     * individual entries warrent an ingest
     * As of 5.11 this method needs revisiting because vector data will be
     * ingested with TimeSeriesIngester and content type detection above will
     * skip over UNKNOWN data.
     * @param dataSource The data source to check
     * @return Whether or not data within the file should be ingested
     * @throws PreIngestException when hashing or id lookup cause some exception
     */
    private FileEvaluation shouldIngest( DataSource dataSource,
                                         DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );

        DataSource.DataDisposition disposition = dataSource.getDisposition();

        // Archives perform their own ingest verification
        if ( disposition == GZIP )
        {
            LOGGER.debug(
                    "The data at '{}' will be ingested because it has been "
                    +
                    "determined that it is an archive that will need to " +
                    "be further evaluated.",
                    dataSource.getUri() );
            return new FileEvaluation( null,
                                       SourceStatus.REQUIRES_DECOMPOSITION,
                                       KEY_NOT_FOUND );
        }

        boolean ingest = ( disposition != UNKNOWN );
        SourceStatus sourceStatus = null;

        String hash;

        if ( ingest )
        {
            try
            {
                // If the format is Netcdf, we want to possibly bypass traditional hashing
                if ( disposition == NETCDF_GRIDDED )
                {
                    hash = NetCDF.getUniqueIdentifier( dataSource.getUri(),
                                                       dataSource.getVariable()
                                                                 .getValue() );
                }
                else
                {
                    // As of 2020-06-02, readers all use TimeSeriesIngester
                    // which hashes the TimeSeries instances individually rather
                    // than files, no need to hash the file at all here now.
                    hash = null;
                }

                sourceStatus = querySourceStatus( hash, lockManager );

                // Added in debugging #58715-116
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Determined that a source with URI {} and "
                                  + "hash {} has the status of "
                                  + "{}",
                                  dataSource.getUri(),
                                  hash,
                                  sourceStatus );
                }
            }
            catch ( IOException ioe )
            {
                throw new PreIngestException(
                        "Could not determine whether to ingest '"
                        + dataSource.getUri() + "'",
                        ioe );
            }
        }
        else
        {
            LOGGER.debug( "The file at '{}' will not be ingested because {}{}",
                          dataSource.getUri(),
                          "it has data that could not be detected as readable ",
                          "by WRES" );
            return new FileEvaluation( null,
                                       SourceStatus.INVALID,
                                       KEY_NOT_FOUND );
        }

        if ( sourceStatus == null )
        {
            throw new IllegalStateException(
                    "Expected sourceStatus to always be set by now." );
        }

        // Get the surrogate key if it exists
        final long surrogateKey;
        Long dataSourceKey = null;
        DataSources dataSourcesCache = this.getDataSourcesCache();

        if ( Objects.nonNull( hash ) )
        {
            try
            {
                dataSourceKey = dataSourcesCache.getActiveSourceID( hash );
            }
            catch ( SQLException se )
            {
                throw new PreIngestException( "While determining if source '"
                                              + dataSource.getUri()
                                              + "' should be ingested, "
                                              + "failed to translate natural key '"
                                              + hash + "' to surrogate key.",
                                              se );
            }
        }

        if ( Objects.isNull( dataSourceKey ) )
        {
            surrogateKey = KEY_NOT_FOUND;
        }
        else
        {
            surrogateKey = dataSourceKey;
        }

        return new FileEvaluation( hash, sourceStatus, surrogateKey );
    }

    /**
     * Determines if the indicated data is currently being ingested by a task
     * in another process.
     * @param hash The hash of the data that some task might be ingesting, known
     *             to exist already.
     * @return true if a task is detected to be ingesting, false otherwise
     * @throws SQLException When communication with the database fails.
     */
    private boolean ingestInProgress( String hash,
                                      DatabaseLockManager lockManager )
            throws SQLException
    {
        DataSources dataSourcesCache = this.getDataSourcesCache();
        SourceDetails sourceDetails = dataSourcesCache.getExistingSource( hash );
        Long sourceId = sourceDetails.getId();
        return lockManager.isSourceLocked( sourceId );
    }

    /**
     * Determines if the indicated data already exists within the database
     * @param hash The hash of the file that might need to be ingested
     * @return Whether or not another task has claimed responsibility for data
     * @throws SQLException Thrown if communcation with the database failed in
     * some way
     */
    private boolean anotherTaskIsResponsibleForSource( String hash )
            throws SQLException
    {
        DataSources dataSourcesCache = this.getDataSourcesCache();
        return dataSourcesCache.hasSource( hash );
    }


    /**
     * Returns true when data ingest of a source is complete, false otherwise.
     * @param hash the data to look for
     * @return Whether the data has been completely ingested.
     * @throws SQLException when query fails
     * @throws NullPointerException when the caller failed to verify that a
     * task already claimed the hash passed in by calling
     * anotherTaskIsResponsibleForSource()
     */
    private boolean wasSourceCompleted( String hash )
            throws SQLException
    {
        DataSources dataSourcesCache = this.getDataSourcesCache();
        SourceDetails details = dataSourcesCache.getExistingSource( hash );
        Database database = this.getDatabase();
        SourceCompletedDetails completedDetails = new SourceCompletedDetails( database,
                                                                              details );
        return completedDetails.wasCompleted();
    }


    /**
     * Attempt to retry a source that either failed or another task was doing.
     *
     * If the source has fully completed, return with "no retry needed, done."
     * If the source was abandoned, delete the old data, return the result of
     * a new attempt to ingest.
     * If the source was neither complete nor abandoned, return that another
     * retry is required.
     * @param ingestResult the old result
     * @return a new list of future results of the retried source
     */

    public List<CompletableFuture<List<IngestResult>>> retry( IngestResult ingestResult )
    {
        if ( !ingestResult.requiresRetry() )
        {
            throw new IllegalArgumentException( "Only IngestResult instances claiming to need retry should be passed." );
        }

        LOGGER.info( "Attempting retry of {}.", ingestResult );
        // Admittedly not optimal to alternate between hash and key, but other
        // places are using querySourceStatus with the hash.
        long surrogateKey = ingestResult.getSurrogateKey();
        DataSources dataSourcesCache = this.getDataSourcesCache();
        String hash = SourceLoader.getHashFromSurrogateKey( dataSourcesCache, surrogateKey );
        SourceStatus sourceStatus = querySourceStatus( hash,
                                                       this.lockManager );

        if ( sourceStatus.equals( SourceStatus.COMPLETED ) )
        {
            LOGGER.debug( "Already finished source {}, changing to say requiresRetry=false",
                          ingestResult );
            CompletableFuture<List<IngestResult>> futureResult =
                    IngestResult.fakeFutureSingleItemListFrom( this.projectConfig,
                                                               ingestResult.getDataSource(),
                                                               ingestResult.getSurrogateKey(),
                                                               false );
            return List.of( futureResult );
        }
        else if ( sourceStatus.equals( SourceStatus.INCOMPLETE_WITH_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING ) )
        {
            LOGGER.debug(
                    "Source {} with status {} not fully ingested, attempting to remove data.",
                    ingestResult,
                    sourceStatus );

            // Need the hash but we only have a surrogate key. Get the hash.

            // First, try to safely remove it:
            boolean removed = IncompleteIngest.removeSourceDataSafely( this.getDatabase(),
                                                                       this.getDataSourcesCache(),
                                                                       ingestResult.getSurrogateKey(),
                                                                       this.lockManager );
            if ( removed )
            {
                LOGGER.debug( "Successfully removed abandoned data source {}, creating new ingest task.",
                              ingestResult );
                return this.ingestData( ingestResult.getDataSource(),
                                        this.projectConfig,
                                        this.lockManager );
            }
            else
            {
                LOGGER.debug( "Failed to remove source {}, will examine again next retry.",
                              ingestResult );
            }
        }

        // For whatever reason, retry is required.
        CompletableFuture<List<IngestResult>> futureResult =
                IngestResult.fakeFutureSingleItemListFrom( this.projectConfig,
                                                           ingestResult.getDataSource(),
                                                           ingestResult.getSurrogateKey(),
                                                           true );
        return List.of( futureResult );
    }

    /**
     * A result of file evaluation containing whether the file was valid,
     * whether the file should be ingested, and the hash if available.
     */
    private static class FileEvaluation
    {
        private final String hash;
        private final SourceStatus sourceStatus;
        private final long surrogateKey;

        FileEvaluation( String hash,
                        SourceStatus sourceStatus,
                        long surrogateKey )
        {
            this.hash = hash;
            this.sourceStatus = sourceStatus;
            this.surrogateKey = surrogateKey;
        }

        public boolean isValid()
        {
            return !this.sourceStatus.equals( SourceStatus.INVALID );
        }

        boolean shouldIngest()
        {
            return this.sourceStatus.equals( SourceStatus.INCOMPLETE_WITH_NO_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING )
                    || this.sourceStatus.equals( SourceStatus.REQUIRES_DECOMPOSITION );
        }

        public String getHash()
        {
            return this.hash;
        }

        boolean ingestMarkedComplete()
        {
            return this.sourceStatus.equals( SourceStatus.COMPLETED );
        }

        boolean ingestInProgress()
        {
            return this.sourceStatus.equals( SourceStatus.INCOMPLETE_WITH_TASK_CLAIMING_AND_TASK_CURRENTLY_INGESTING );
        }

        SourceStatus getSourceStatus()
        {
            return this.sourceStatus;
        }

        long getSurrogateKey()
        {
            return this.surrogateKey;
        }
    }

    /**
     * <p>Evaluates a project and creates a {@link DataSource} for each 
     * distinct source within the project that needs to
     * be loaded, together with any additional links required. A link is required 
     * for each additional context, i.e. {@link LeftOrRightOrBaseline}, in 
     * which the source appears. The links are returned by {@link DataSource#getLinks()}.
     * Here, a "link" means a separate entry in <code>wres.ProjectSource</code>.
     * 
     * <p>A {@link DataSource} is returned for each discrete source. When the declared
     * {@link DataSourceConfig.Source} points to a directory of files, the tree 
     * is walked and a {@link DataSource} is returned for each one within the tree 
     * that meets any prescribed filters.
     * 
     * @return the set of distinct sources to load and any additional links to create
     * @throws NullPointerException if the input is null
     */

    private static Set<DataSource> createSourcesToLoadAndLink( SystemSettings systemSettings,
                                                               ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );

        // Somewhat convoluted structure that will be turned into a simple one.
        // The key is the distinct source, and the paired value is the context in
        // which the source appears and the set of additional links to create, if any.
        // Note that all project declaration overrides hashCode and equals (~ key in a HashMap)
        Map<DataSourceConfig.Source, Pair<DataSourceConfig, Set<LeftOrRightOrBaseline>>> sources = new HashMap<>();

        // Must have one or more left sources to load and link
        SourceLoader.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getLeft() );

        // Must have one or more right sources to load and link
        SourceLoader.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getRight() );

        // May have one or more baseline sources to load and link
        if ( Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
        {
            SourceLoader.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getBaseline() );
        }

        // Create a simple entry (DataSource) for each complex entry
        Set<DataSource> returnMe = new HashSet<>();

        // Expand any file sources that represent directories and filter any that are not required
        for ( Map.Entry<DataSourceConfig.Source, Pair<DataSourceConfig, Set<LeftOrRightOrBaseline>>> nextSource : sources.entrySet() )
        {
            // Evaluate the path, which is null for a source that is not file-like
            Path path = SourceLoader.evaluatePath( systemSettings, nextSource.getKey() );

            // Allow GC of new empty Sets by letting the links ref empty set.
            Set<LeftOrRightOrBaseline> links = Collections.emptySet();

            if ( !nextSource.getValue()
                            .getRight()
                            .isEmpty() )
            {
                links = nextSource.getValue()
                                  .getRight();
            }

            // If there is a file-like source, test for a directory and decompose it as required
            if( Objects.nonNull( path ) )
            {
                DataSource source = DataSource.of( FILE_OR_DIRECTORY,
                                                   nextSource.getKey(),
                                                   nextSource.getValue()
                                                             .getLeft(),
                                                   links,
                                                   path.toUri() );

                returnMe.addAll( SourceLoader.decomposeFileSource( source ) );
            }
            // Not a file-like source
            else
            {
                DataSource source = DataSource.of( COMPLEX,
                                                   nextSource.getKey(),
                                                   nextSource.getValue()
                                                             .getLeft(),
                                                   links,
                                                   nextSource.getKey()
                                                             .getValue() );
                returnMe.add( source );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Helper that decomposes a file-like source into other sources. In particular, 
     * if the declared source represents a directory, walk the tree and find sources 
     * that match any prescribed pattern. Return a {@link DataSource}
     * 
     * @param dataSource the source to decompose
     * @return the set of decomposed sources
     */
    
    private static Set<DataSource> decomposeFileSource( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        
        // Look at the path to see whether it maps to a directory
        Path sourcePath = Paths.get( dataSource.getUri() );

        File file = sourcePath.toFile();
        
        Set<DataSource> returnMe = new HashSet<>();
        
        // Directory: must decompose into sources
        if( file.isDirectory() )
        {

            DataSourceConfig.Source source = dataSource.getSource();
            
            //Define path matcher based on the source's pattern, if provided.
            final PathMatcher matcher;
            
            String pattern = source.getPattern();
                    
            if ( !com.google.common.base.Strings.isNullOrEmpty( pattern ) )
            {
                matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern );
            }
            else
            {
                matcher = null;
            }
            
            // Walk the tree and find sources that match a pattern or none
            try ( Stream<Path> files = Files.walk( sourcePath ) )
            {            
                files.forEach( path -> {

                    File testFile = path.toFile();

                    //File must be a file and match the pattern, if the pattern is defined.
                    if ( testFile.isFile() && ( ( matcher == null ) || matcher.matches( path ) ) )
                    {
                        DataSource.DataDisposition disposition = DataSource.detectFormat( path.toUri() );

                        if ( disposition != UNKNOWN )
                        {
                            returnMe.add( DataSource.of( disposition,
                                                         dataSource.getSource(),
                                                         dataSource.getContext(),
                                                         dataSource.getLinks(),
                                                         path.toUri() ) );
                        }
                        else
                        {
                            LOGGER.warn( "Skipping '{}' because WRES cannot read it.",
                                         path );
                        }
                    }
                    // Skip and log a warning if this is a normal file (e.g. not a directory) 
                    else if ( testFile.isFile() )
                    {
                        LOGGER.warn( "Skipping {} because it does not match pattern \"{}\".",
                                     path,
                                     pattern );
                    }
                } );
            }
            catch ( IOException e )
            {
                throw new PreIngestException( "Failed to walk the directory tree '"
                                              + sourcePath + "':", e );
            }

            //If the results are empty, then there were either no files in the specified source or pattern matched 
            //none of the files.  
            if ( returnMe.isEmpty() )
            {
                throw new PreIngestException( "The pattern of \"" + pattern
                                              + "\" does not yield any files within the provided "
                                              + "source path and is therefore not a valid source." );
            }
            
        }
        else
        {
            DataSource.DataDisposition disposition = DataSource.detectFormat( dataSource.getUri() );
            DataSource withDisposition = DataSource.of( disposition,
                                                        dataSource.getSource(),
                                                        dataSource.getContext(),
                                                        dataSource.getLinks(),
                                                        dataSource.getUri() );
            returnMe.add( withDisposition );
        }
        
        return Collections.unmodifiableSet( returnMe );
        
    }
    
    /**
     * Evaluate a path from a {@link DataSourceConfig.Source}.
     * @param source the source
     * @return the path of a file-like source or null
     */
    
    private static Path evaluatePath( SystemSettings systemSettings,
                                      DataSourceConfig.Source source )
    {
        LOGGER.trace( "Called evaluatePath with source {}", source );
        URI uri = source.getValue();

        if ( source.getInterface() != null )
        {
            LOGGER.debug( "There is an interface specified: {}, therefore not going to walk a directory tree.",
                          source.getInterface() );
            return null;
        }

        // Is there a source path to evaluate? Only if the source is file-like
        if( uri.toString().isEmpty() )
        {
            LOGGER.debug( "The source value was empty from source {}", source );
            return null;           
        }

        String scheme = uri.getScheme();

        if ( scheme != null
             && !scheme.toLowerCase()
                       .equals( "file" ) )
        {
            LOGGER.debug( "Scheme '{}' indicates non-file.", scheme );
            return null;
        }

        Path sourcePath;

        // Construct a path using the SystemSetting wres.dataDirectory when
        // the specified source is not absolute.
        if ( !uri.isAbsolute() )
        {
            sourcePath = systemSettings.getDataDirectory()
                                       .resolve( uri.getPath() );
            LOGGER.debug( "Transformed relative URI {} to Path {}.",
                          uri,
                          sourcePath );
        }
        else
        {
            sourcePath = Paths.get( uri );
        }

        LOGGER.debug( "Returning source path {} from source {}",
                      sourcePath, source.getValue() );
        return sourcePath;
    }
 
    /**
     * Mutates the input map of sources, adding additional sources to load or link
     * from the input {@link DataSourceConfig}.
     * 
     * @param sources the map of sources to mutate
     * @param projectConfig the project configuration
     * @param dataSourceConfig the data source configuration for which sources to load or link are required
     * @throws NullPointerException if any input is null
     */

    private static void
            mutateSourcesToLoadAndLink( Map<DataSourceConfig.Source, Pair<DataSourceConfig, Set<LeftOrRightOrBaseline>>> sources,
                                        ProjectConfig projectConfig,
                                        DataSourceConfig dataSourceConfig )
    {
        Objects.requireNonNull( sources );

        Objects.requireNonNull( projectConfig );

        Objects.requireNonNull( dataSourceConfig );

        LeftOrRightOrBaseline sourceType = ConfigHelper.getLeftOrRightOrBaseline( projectConfig, dataSourceConfig );

        // Must have one or more right sources
        for ( DataSourceConfig.Source source : dataSourceConfig.getSource() )
        {
            // Link or load?
            // NOTE: there are some paired contexts in which it would be wrong for
            // the source to appear together (e.g. both left and right),
            // but that validation needs to happen way before now, so proceed in all cases

            // Only create a link if the source is already in the load list/map
            if ( sources.containsKey( source ) )
            {
                // Only link sources that appear in a different context
                if ( ConfigHelper.getLeftOrRightOrBaseline( projectConfig,
                                                            sources.get( source )
                                                                   .getLeft() ) != sourceType )
                {
                    sources.get( source ).getRight().add( sourceType );
                }
            }
            // Load
            else
            {
                sources.put( source, Pair.of( dataSourceConfig, new HashSet<>() ) );
            }
        }

    }


    /**
     * Returns the likely status of a given source hash based on state in db.
     * @param hash The natural identifier of the source, null if not known yet.
     * @param lockManager The lock manager to use to query status.
     * @return the source status
     * @throws PreIngestException When communication with the database fails.
     */

    SourceStatus querySourceStatus( String hash, DatabaseLockManager lockManager )
    {
        boolean anotherTaskStartedIngest = false;
        boolean ingestMarkedComplete = false;
        boolean ingestInProgress = false;

        if ( Objects.isNull( hash ) )
        {
            // When the hash is null, report it as not started, etc.
            // As of 2020-06-02, TimeSeriesIngester will investigate status,
            // so do not bother checking here for files.
            return SourceStatus.INCOMPLETE_WITH_NO_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING;
        }

        try
        {
            anotherTaskStartedIngest = anotherTaskIsResponsibleForSource( hash );

            if ( anotherTaskStartedIngest )
            {
                ingestMarkedComplete = wasSourceCompleted( hash );

                if ( ingestMarkedComplete )
                {
                    return SourceStatus.COMPLETED;
                }
                else
                {
                    LOGGER.debug( "Another task is responsible for {} but has not yet finished it.",
                                  hash );
                    ingestInProgress = ingestInProgress( hash, lockManager );
                    LOGGER.debug( "Is another task currently ingesting {}? {}",
                                  hash, ingestInProgress );
                    if ( ingestInProgress )
                    {
                        return SourceStatus.INCOMPLETE_WITH_TASK_CLAIMING_AND_TASK_CURRENTLY_INGESTING;
                    }
                    else
                    {
                        return SourceStatus.INCOMPLETE_WITH_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING;
                    }
                }
            }
            else
            {
                // No task claimed this source as of a moment ago
                return SourceStatus.INCOMPLETE_WITH_NO_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING;
            }
        }
        catch ( SQLException se )
        {
            throw new PreIngestException( "Unable to query status of the source identified by "
                                          + hash, se );
        }
    }


    /**
     * Return the natural id from a given database-specific source row id.
     * @param surrogateKey The surrogate key id.
     * @return The hash.
     * @throws PreIngestException When database calls fail or cache fails.
     */

    private static String getHashFromSurrogateKey( DataSources dataSourcesCache,
                                                   long surrogateKey )
    {
        SourceDetails details;

        try
        {
            details = dataSourcesCache.getFromCacheOrDatabaseByIdThenCache( surrogateKey );
        }
        catch ( SQLException se )
        {
            throw new PreIngestException( "While looking for natural id of source_id '"
                                          + surrogateKey, se );
        }

        if ( Objects.nonNull( details ) )
        {
            String hash = details.getHash();

            if ( Objects.nonNull( hash )
                 && !hash.isBlank() )
            {
                return hash;
            }
            else
            {
                throw new PreIngestException( "DataSources cache returned a null hash for data identified by surrogate key "
                                              + surrogateKey );
            }
        }
        else
        {
            throw new PreIngestException( "Unable to find natural id for data identified by surrogate key "
                                          + surrogateKey );
        }
    }
}
