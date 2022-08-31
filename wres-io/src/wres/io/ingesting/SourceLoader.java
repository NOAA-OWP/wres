package wres.io.ingesting;

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
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.GriddedFeatures;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.ReaderFactory;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.Source;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.nwm.NWMReader;
import wres.io.reading.web.WebSource;
import wres.io.removal.IncompleteIngest;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.SystemSettings;
import wres.util.NetCDF;

/**
 * Evaluates datasources specified within a project configuration and determines
 * what data should be ingested. Asynchronous tasks for each file needed for
 * ingest are created and sent to the Exector for ingestion.
 * @author Christopher Tubbs
 * @author James Brown
 * @author Jesse Bickel
 */
public class SourceLoader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SourceLoader.class );
    private static final long KEY_NOT_FOUND = Long.MIN_VALUE;

    private final SystemSettings systemSettings;
    private final ExecutorService executor;
    private final Database database;
    private final DatabaseCaches caches;
    private final GriddedFeatures.Builder griddedFeatures;
    private final TimeSeriesIngester timeSeriesIngester;

    /**
     * The project configuration indicating what data to use
     */
    private final ProjectConfig projectConfig;
    private final DatabaseLockManager lockManager;

    /** An enumeration of the ingest status of a source. */
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
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings The system settings
     * @param executor The executor
     * @param database The database
     * @param caches The caches
     * @param projectConfig the project configuration
     * @param lockManager the tool to manage ingest locks, shared per ingest
     * @param griddedFeatures the gridded features cache to populate, if required
     * @throws NullPointerException if any required input is null
     */
    public SourceLoader( TimeSeriesIngester timeSeriesIngester,
                         SystemSettings systemSettings,
                         ExecutorService executor,
                         Database database,
                         DatabaseCaches caches,
                         ProjectConfig projectConfig,
                         DatabaseLockManager lockManager,
                         GriddedFeatures.Builder griddedFeatures )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( projectConfig );

        if ( !systemSettings.isInMemory() )
        {
            Objects.requireNonNull( database );
            Objects.requireNonNull( caches );
            Objects.requireNonNull( lockManager );

            this.lockManager = lockManager;
        }
        else
        {
            this.lockManager = new DatabaseLockManagerNoop();
        }

        this.systemSettings = systemSettings;
        this.executor = executor;
        this.database = database;
        this.caches = caches;
        this.projectConfig = projectConfig;
        this.timeSeriesIngester = timeSeriesIngester;
        this.griddedFeatures = griddedFeatures;
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
        for ( DataSource source : sources )
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
        CompletableFuture<List<IngestResult>> nonFileIngest = this.loadNonFileSource( source );

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
                 || interfaceShortHand.equals( InterfaceShortHand.WRDS_OBS )
                 || interfaceShortHand.equals( InterfaceShortHand.USGS_NWIS )
                 || interfaceShortHand.equals( InterfaceShortHand.WRDS_NWM ) )
            {
                WebSource webSource = WebSource.of( this.getTimeSeriesIngester(),
                                                    this.getSystemSettings(),
                                                    this.getDatabase(),
                                                    this.getCaches(),
                                                    this.getProjectConfig(),
                                                    source,
                                                    this.getLockManager() );
                return CompletableFuture.supplyAsync( webSource::call,
                                                      this.getExecutor() );
            }
            else
            {
                // Must be NWM, right?
                NWMReader nwmReader = new NWMReader( this.getTimeSeriesIngester(),
                                                     this.getSystemSettings(),
                                                     this.getProjectConfig(),
                                                     source );
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
            WebSource webSource = WebSource.of( this.getTimeSeriesIngester(),
                                                this.getSystemSettings(),
                                                this.getDatabase(),
                                                this.getCaches(),
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

        Source reader = ReaderFactory.getReader( this.getTimeSeriesIngester(),
                                                 this.getSystemSettings(),
                                                 this.getDatabase(),
                                                 this.getCaches(),
                                                 projectConfig,
                                                 source,
                                                 lockManager );

        task = CompletableFuture.supplyAsync( reader::save, this.getExecutor() );

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
        FileEvaluation checkIngest = this.shouldIngest( source,
                                                        lockManager );
        SourceStatus sourceStatus = checkIngest.getSourceStatus();

        if ( sourceStatus == SourceStatus.REQUIRES_DECOMPOSITION
             || sourceStatus == SourceStatus.INCOMPLETE_WITH_NO_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING
             || sourceStatus == SourceStatus.INCOMPLETE_WITH_TASK_CLAIMING_AND_TASK_CURRENTLY_INGESTING )
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
                                                     this.getCaches()
                                                         .getDataSourcesCache(),
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
            tasks.add( IngestResult.fakeFutureSingleItemListFrom( source,
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

        // Ingest gridded metadata when required
        if ( source.getDisposition() == DataDisposition.NETCDF_GRIDDED && !this.getSystemSettings()
                                                                               .isInMemory() )
        {
            Supplier<List<IngestResult>> fakeResult = this.ingestGriddedFeatures( source );
            CompletableFuture<List<IngestResult>> future =
                    CompletableFuture.supplyAsync( fakeResult,
                                                   this.getExecutor() );
            tasks.add( future );
        }

        return Collections.unmodifiableList( tasks );
    }

    /**
     * @param source the gridded netcdf source to read
     */

    private Supplier<List<IngestResult>> ingestGriddedFeatures( DataSource source )
    {
        return () -> {
            try ( NetcdfFile ncf = NetcdfFiles.open( source.getUri().toString() ) )
            {
                this.getGriddedFeatures()
                    .addFeatures( ncf );
            }
            catch ( IOException e )
            {
                throw new IngestException( "While ingesting features for Netcdf gridded file " + source.getUri()
                                           + "." );
            }

            // Fake, no ingest results
            return List.of();
        };
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

        DataDisposition disposition = dataSource.getDisposition();

        // Archives perform their own ingest verification
        if ( disposition == DataDisposition.GZIP )
        {
            LOGGER.debug( "The data at '{}' will be ingested because it has been determined that it is an archive that "
                          + "will need to be further evaluated.",
                          dataSource.getUri() );
            return new FileEvaluation( SourceStatus.REQUIRES_DECOMPOSITION,
                                       KEY_NOT_FOUND );
        }

        // Is this in-memory, i.e., no ingest required?
        if ( this.systemSettings.isInMemory() )
        {
            return new FileEvaluation( SourceStatus.INCOMPLETE_WITH_NO_TASK_CLAIMING_AND_NO_TASK_CURRENTLY_INGESTING,
                                       KEY_NOT_FOUND );
        }

        boolean ingest = ( disposition != DataDisposition.UNKNOWN );
        SourceStatus sourceStatus = null;

        String hash;

        if ( ingest )
        {
            try
            {
                // If the format is Netcdf, we want to possibly bypass traditional hashing
                if ( disposition == DataDisposition.NETCDF_GRIDDED )
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

                sourceStatus = this.querySourceStatus( hash, lockManager );

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
                                              + dataSource.getUri()
                                              + "'",
                                              ioe );
            }
        }
        else
        {
            LOGGER.debug( "The file at '{}' will not be ingested because {}{}",
                          dataSource.getUri(),
                          "it has data that could not be detected as readable ",
                          "by WRES" );
            return new FileEvaluation( SourceStatus.INVALID,
                                       KEY_NOT_FOUND );
        }

        if ( sourceStatus == null )
        {
            throw new IllegalStateException( "Expected sourceStatus to always be set by now." );
        }

        // Get the surrogate key if it exists
        long surrogateKey = this.getSurrogateKey( hash, dataSource.getUri() );

        return new FileEvaluation( sourceStatus, surrogateKey );
    }

    /**
     * @param hash the hash
     * @param uri the uri
     * @return the surrogate key or {@link #KEY_NOT_FOUND}
     */
    private long getSurrogateKey( String hash, URI uri )
    {
        // Get the surrogate key if it exists
        Long dataSourceKey = null;
        if ( Objects.nonNull( hash ) )
        {
            try
            {
                DataSources dataSources = this.getCaches()
                                              .getDataSourcesCache();
                dataSourceKey = dataSources.getActiveSourceID( hash );
            }
            catch ( SQLException se )
            {
                throw new PreIngestException( "While determining if source '"
                                              + uri
                                              + "' should be ingested, "
                                              + "failed to translate natural key '"
                                              + hash
                                              + "' to surrogate key.",
                                              se );
            }
        }

        if ( Objects.nonNull( dataSourceKey ) )
        {
            return dataSourceKey;
        }

        return KEY_NOT_FOUND;
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
        DataSources dataSources = this.getCaches()
                                      .getDataSourcesCache();
        SourceDetails sourceDetails = dataSources.getExistingSource( hash );
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
        DataSources dataSources = this.getCaches()
                                      .getDataSourcesCache();
        return dataSources.hasSource( hash );
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
        DataSources dataSources = this.getCaches()
                                      .getDataSourcesCache();
        SourceDetails details = dataSources.getExistingSource( hash );
        Database db = this.getDatabase();
        SourceCompletedDetails completedDetails = new SourceCompletedDetails( db,
                                                                              details );
        return completedDetails.wasCompleted();
    }

    /**
     * A result of file evaluation.
     */
    private static class FileEvaluation
    {
        private final SourceStatus sourceStatus;
        private final long surrogateKey;

        FileEvaluation( SourceStatus sourceStatus,
                        long surrogateKey )
        {
            this.sourceStatus = sourceStatus;
            this.surrogateKey = surrogateKey;
        }

        boolean ingestMarkedComplete()
        {
            return this.sourceStatus.equals( SourceStatus.COMPLETED );
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
        Map<DataSourceConfig.Source, Pair<DataSourceConfig, List<LeftOrRightOrBaseline>>> sources = new HashMap<>();

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
        for ( Map.Entry<DataSourceConfig.Source, Pair<DataSourceConfig, List<LeftOrRightOrBaseline>>> nextSource : sources.entrySet() )
        {
            // Evaluate the path, which is null for a source that is not file-like
            Path path = SourceLoader.evaluatePath( systemSettings, nextSource.getKey() );

            // Allow GC of new empty Sets by letting the links ref empty set.
            List<LeftOrRightOrBaseline> links = Collections.emptyList();

            if ( !nextSource.getValue()
                            .getRight()
                            .isEmpty() )
            {
                links = nextSource.getValue()
                                  .getRight();
            }

            DataSourceConfig dataSourceConfig = nextSource.getValue()
                                                          .getLeft();

            LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( projectConfig, dataSourceConfig );

            // If there is a file-like source, test for a directory and decompose it as required
            if ( Objects.nonNull( path ) )
            {
                // Currently unknown disposition, to be unpacked/determined
                DataSource source = DataSource.of( DataDisposition.UNKNOWN,
                                                   nextSource.getKey(),
                                                   dataSourceConfig,
                                                   links,
                                                   path.toUri(),
                                                   lrb );

                Set<DataSource> filesources = SourceLoader.decomposeFileSource( source );
                returnMe.addAll( filesources );
            }
            // Not a file-like source
            else
            {
                // Create a source with unknown disposition as the basis for detection
                DataSource source = DataSource.of( DataDisposition.UNKNOWN,
                                                   nextSource.getKey(),
                                                   dataSourceConfig,
                                                   links,
                                                   nextSource.getKey()
                                                             .getValue(),
                                                   lrb );

                DataDisposition disposition = SourceLoader.getDispositionOfNonFileSource( source );
                DataSource.of( disposition,
                               nextSource.getKey(),
                               dataSourceConfig,
                               links,
                               nextSource.getKey()
                                         .getValue(),
                               lrb );

                returnMe.add( source );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns the disposition of a non-file like source, which includes all web sources.
     * 
     * @param dataSource the existing data source whose disposition is unknown
     */

    private static DataDisposition getDispositionOfNonFileSource( DataSource dataSource )
    {
        if ( ReaderUtilities.isWebSource( dataSource ) )
        {
            if ( ReaderUtilities.isUsgsSource( dataSource ) )
            {
                return DataDisposition.JSON_WATERML;
            }
            else if ( ReaderUtilities.isWrdsAhpsSource( dataSource )
                      || ReaderUtilities.isWrdsObservedSource( dataSource ) )
            {
                return DataDisposition.JSON_WRDS_AHPS;
            }
            else if ( ReaderUtilities.isWrdsNwmSource( dataSource ) )
            {
                return DataDisposition.JSON_WRDS_NWM;
            }
            // Hosted NWM data, not via a WRDS API
            else if( ReaderUtilities.isNwmVectorSource( dataSource ) )
            {
                return DataDisposition.NETCDF_VECTOR;
            }
        }

        throw new UnsupportedOperationException( "Discovered an unsupported data source: "
                                                 + dataSource
                                                 + "." );
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

        // Special case: NWM vector sources do not require further decomposition, merely the correct disposition
        if ( ReaderUtilities.isNwmVectorSource( dataSource ) )
        {
            LOGGER.debug( "Discovered a source with disposition {}.", DataDisposition.NETCDF_VECTOR );
            DataSource innerSource = DataSource.of( DataDisposition.NETCDF_VECTOR,
                                                    dataSource.getSource(),
                                                    dataSource.getContext(),
                                                    dataSource.getLinks(),
                                                    dataSource.getUri(),
                                                    dataSource.getLeftOrRightOrBaseline() );
            return Set.of( innerSource );
        }

        // Directory: must decompose into sources
        if ( file.isDirectory() )
        {
            return SourceLoader.decomposeDirectorySource( dataSource );
        }
        else
        {
            DataDisposition disposition = DataSource.detectFormat( dataSource.getUri() );
            DataSource withDisposition = DataSource.of( disposition,
                                                        dataSource.getSource(),
                                                        dataSource.getContext(),
                                                        dataSource.getLinks(),
                                                        dataSource.getUri(),
                                                        dataSource.getLeftOrRightOrBaseline() );
            return Set.of( withDisposition );
        }
    }

    /**
     * Helper that decomposes a file-like source into other sources. In particular, 
     * if the declared source represents a directory, walk the tree and find sources 
     * that match any prescribed pattern. Return a {@link DataSource}
     * 
     * @param dataSource the source to decompose
     * @return the set of decomposed sources
     */

    private static Set<DataSource> decomposeDirectorySource( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        Path sourcePath = Paths.get( dataSource.getUri() );

        Set<DataSource> returnMe = new HashSet<>();

        DataSourceConfig.Source source = dataSource.getSource();

        //Define path matcher based on the source's pattern, if provided.
        final PathMatcher matcher;

        String pattern = source.getPattern();

        if ( ! ( pattern == null || pattern.isEmpty() ) )
        {
            matcher = FileSystems.getDefault().getPathMatcher( "glob:" + pattern );
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
                    DataDisposition disposition = DataSource.detectFormat( path.toUri() );

                    if ( disposition != DataDisposition.UNKNOWN )
                    {
                        returnMe.add( DataSource.of( disposition,
                                                     dataSource.getSource(),
                                                     dataSource.getContext(),
                                                     dataSource.getLinks(),
                                                     path.toUri(),
                                                     dataSource.getLeftOrRightOrBaseline() ) );
                    }
                    else
                    {
                        LOGGER.warn( "Skipping '{}' because WRES will not be able to parse it.",
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
                                          + sourcePath
                                          + "':",
                                          e );
        }

        //If the results are empty, then there were either no files in the specified source or pattern matched 
        //none of the files.  
        if ( returnMe.isEmpty() )
        {
            throw new PreIngestException( "The pattern of \"" + pattern
                                          + "\" does not yield any files within the provided "
                                          + "source path and is therefore not a valid source." );
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

        // Interface defined and not a source of NWM vectors, so no path to return
        if ( source.getInterface() != null && !source.getInterface()
                                                     .name()
                                                     .toLowerCase()
                                                     .startsWith( "nwm_" ) )
        {
            LOGGER.debug( "There is an interface specified: {}, therefore not going to walk a directory tree.",
                          source.getInterface() );
            return null;
        }

        // Is there a source path to evaluate? Only if the source is file-like
        if ( uri.toString().isEmpty() )
        {
            LOGGER.debug( "The source value was empty from source {}", source );
            return null;
        }

        String scheme = uri.getScheme();

        if ( scheme != null
             && !scheme.equalsIgnoreCase( "file" ) )
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
                      sourcePath,
                      source.getValue() );
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
            mutateSourcesToLoadAndLink( Map<DataSourceConfig.Source, Pair<DataSourceConfig, List<LeftOrRightOrBaseline>>> sources,
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
                sources.get( source ).getRight().add( sourceType );
            }
            // Load
            else
            {
                sources.put( source, Pair.of( dataSourceConfig, new ArrayList<>() ) );
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

    private SourceStatus querySourceStatus( String hash, DatabaseLockManager lockManager )
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
            anotherTaskStartedIngest = this.anotherTaskIsResponsibleForSource( hash );

            if ( anotherTaskStartedIngest )
            {
                ingestMarkedComplete = this.wasSourceCompleted( hash );

                if ( ingestMarkedComplete )
                {
                    return SourceStatus.COMPLETED;
                }
                else
                {
                    LOGGER.debug( "Another task is responsible for {} but has not yet finished it.",
                                  hash );
                    ingestInProgress = this.ingestInProgress( hash, lockManager );
                    LOGGER.debug( "Is another task currently ingesting {}? {}",
                                  hash,
                                  ingestInProgress );
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
                                          + hash,
                                          se );
        }
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

    private DatabaseCaches getCaches()
    {
        return this.caches;
    }

    private GriddedFeatures.Builder getGriddedFeatures()
    {
        Objects.requireNonNull( this.griddedFeatures,
                                "Cannot read a gridded dataset without a gridded features cache." );

        return this.griddedFeatures;
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }

}
