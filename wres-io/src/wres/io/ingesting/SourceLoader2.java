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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Caches;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.ReadException;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.TimeSeriesReaderFactory;
import wres.io.reading.TimeSeriesTuple;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.util.NetCDF;

/**
 * This is where reading of time-series formats meets ingesting of time-series (e.g., into a persistent store). Creates 
 * one or more {@link DataSource} for reading and ingesting and then reads and ingests them. Reading in this context 
 * means instantiating a {@link TimeSeriesReader} and supplying the reader with a {@link DataSource} to read, while 
 * ingesting means passing the resulting time-series and its descriptive {@link DataSource} to a 
 * {@link TimeSeriesIngester}, which is supplied on construction.
 * 
 * TODO: Given that {@link TimeSeriesIngester} is an API that may or may not ingest time-series data into a database, 
 * this class should not make assumptions about the ingest implementation. For example, it should not perform database 
 * operations or use database ORMs/caches.
 * 
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */
public class SourceLoader2
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SourceLoader2.class );
    private static final long KEY_NOT_FOUND = Long.MIN_VALUE;

    private final ExecutorService readingExecutor;

    private final Database database;
    private final Caches caches;
    private final SystemSettings systemSettings;

    /** Time-series ingester. **/
    private final TimeSeriesIngester timeSeriesIngester;

    /** Factory for creating time-series readers. **/
    private final TimeSeriesReaderFactory timeSeriesReaderFactory;

    /**
     * The project configuration indicating what data to use
     */
    private final ProjectConfig projectConfig;

    /** An enumeration of the loading status of a source. */
    private enum SourceStatus
    {
        /** The status of a source that has not been loaded. */
        NOT_COMPLETE,
        /** The status of a source that has been loaded successfully. */
        COMPLETED,
        /** The status of a source that cannot be loaded. */
        INVALID
    }

    /**
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings The system settings
     * @param readingExecutor The executor for reading
     * @param database The database
     * @param caches The caches
     * @param projectConfig the project configuration
     * @throws NullPointerException if any required input is null
     */
    public SourceLoader2( TimeSeriesIngester timeSeriesIngester,
                          SystemSettings systemSettings,
                          ExecutorService readingExecutor,
                          Database database,
                          Caches caches,
                          ProjectConfig projectConfig )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( readingExecutor );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getPair() );

        if ( !systemSettings.isInMemory() )
        {
            Objects.requireNonNull( database );
            Objects.requireNonNull( caches );
        }

        this.systemSettings = systemSettings;
        this.readingExecutor = readingExecutor;
        this.database = database;
        this.caches = caches;
        this.projectConfig = projectConfig;
        this.timeSeriesIngester = timeSeriesIngester;
        this.timeSeriesReaderFactory = TimeSeriesReaderFactory.of( projectConfig.getPair(),
                                                                   this.getSystemSettings(),
                                                                   this.getCaches()
                                                                       .getFeaturesCache() );
    }

    /**
     * Loads the time-series data.
     * 
     * @return List of future ingest results
     * @throws IOException when no data is found
     * @throws IngestException when getting project details fails
     */
    public List<CompletableFuture<List<IngestResult>>> load() throws IOException
    {
        LOGGER.info( "Loading the declared datasets. {}{}{}{}{}{}",
                     "Depending on many factors (including dataset size, ",
                     "dataset design, data service implementation, service ",
                     "availability, network bandwidth, network latency, ",
                     "storage bandwidth, storage latency, concurrent ",
                     "evaluations on shared resources, concurrent computation ",
                     "on shared resources) this can take a while..." );

        // Create the sources for which ingest should be attempted, together with any required links. A link is a 
        // connection between a data source and a context or LeftOrRightOrBaseline. A link is required for each context 
        // in which the source appears within a project.
        Set<DataSource> sources = SourceLoader2.createSourcesToLoadAndLink( this.getSystemSettings(),
                                                                            this.getProjectConfig() );

        LOGGER.debug( "Created these sources to load and link: {}", sources );

        // Load each source and create any additional links required
        List<CompletableFuture<List<IngestResult>>> savingSources = new ArrayList<>();
        for ( DataSource source : sources )
        {
            List<CompletableFuture<List<IngestResult>>> results = this.loadSource( source );
            savingSources.addAll( results );
        }

        return Collections.unmodifiableList( savingSources );
    }

    /**
     * Attempts to load the input source.
     * 
     * @param source The data source
     * @return A listing of asynchronous tasks dispatched to ingest data
     * @throws FileNotFoundException when a source file is not found
     */
    private List<CompletableFuture<List<IngestResult>>> loadSource( DataSource source )
    {
        if ( ReaderUtilities.isWebSource( source ) )
        {
            return this.loadNonFileSource( source );
        }
        else
        {
            return this.loadFileSource( source );
        }
    }

    /**
     * Load data where the source is known to be a file.
     *
     * @param source the source to ingest, must be a file
     * @return a list of future lists of ingest results, possibly empty
     */
    private List<CompletableFuture<List<IngestResult>>> loadFileSource( DataSource source )
    {
        Objects.requireNonNull( source );

        LOGGER.debug( "Attempting to load a file-like source: {}", source );

        List<CompletableFuture<List<IngestResult>>> tasks = new ArrayList<>();

        // Ingest gridded metadata when required - the updated cache will be used by the gridded reader
        if ( source.getDisposition() == DataDisposition.NETCDF_GRIDDED && !this.getSystemSettings()
                                                                               .isInMemory() )
        {
            Supplier<List<IngestResult>> fakeResult = this.ingestGriddedFeatures( source );
            CompletableFuture<List<IngestResult>> future =
                    CompletableFuture.supplyAsync( fakeResult,
                                                   this.getReadingExecutor() );
            tasks.add( future );
            try
            {
                // Get the gridded features upfront
                future.get();
            }
            catch ( ExecutionException e )
            {
                throw new ReadException( "Could not read the gridded features." );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread()
                      .interrupt();

                throw new ReadException( "Could not read the gridded features." );
            }
        }

        URI sourceUri = source.getUri();
        FileEvaluation checkIngest = this.shouldIngestFileSource( source );
        SourceStatus sourceStatus = checkIngest.getSourceStatus();

        if ( sourceStatus == SourceStatus.NOT_COMPLETE )
        {
            List<CompletableFuture<List<IngestResult>>> futureList = this.readAndIngestData( source );
            tasks.addAll( futureList );
        }
        else if ( sourceStatus == SourceStatus.COMPLETED )
        {
            LOGGER.debug( "Data will not be loaded from '{}'. That data was already loaded.",
                          sourceUri );

            // Fake a future, return result immediately, nothing to read/ingest.
            tasks.add( IngestResult.fakeFutureSingleItemListFrom( source,
                                                                  checkIngest.getSurrogateKey(),
                                                                  !checkIngest.ingestMarkedComplete() ) );
        }
        else if ( sourceStatus == SourceStatus.INVALID )
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

        LOGGER.trace( "Returning tasks {} for URI {}.",
                      tasks,
                      sourceUri );

        return Collections.unmodifiableList( tasks );
    }

    /**
     * Loads a web-like source.
     * 
     * TODO: create links for a non-file source when it appears in more than 
     * one context, i.e. {@link LeftOrRightOrBaseline}. 
     * See {@link #loadFileSource(DataSource, ProjectConfig, DatabaseLockManager)}
     * for how this is done with a file source.
     * 
     * See #67774.
     * 
     * @param source the data source
     * @return a single future list of results
     */

    private List<CompletableFuture<List<IngestResult>>> loadNonFileSource( DataSource source )
    {
        LOGGER.debug( "Attempting to load a web-like source: {}", source );

        return this.readAndIngestData( source );
    }

    /**
     * This is where reading meets ingest. Instantiates a reader and then passes the stream through to an ingester.
     * @param source the source to ingest
     * @return a list of future lists of ingest results, possibly empty
     */

    private List<CompletableFuture<List<IngestResult>>> readAndIngestData( DataSource source )
    {
        Objects.requireNonNull( source );

        TimeSeriesReader reader = this.getTimeSeriesReaderFactory()
                                      .getReader( source );

        TimeSeriesIngester ingester = this.getTimeSeriesIngester();

        // As of 20220824, grids are not read at "read" time unless there is an in-memory evaluation. See #51232.
        if ( source.isGridded() && !this.getSystemSettings()
                                        .isInMemory() )
        {
            // Empty stream, which will trigger source ingest only, not time-series reading/ingest
            Stream<TimeSeriesTuple> emptyStream = Stream.of();

            CompletableFuture<List<IngestResult>> task =
                    CompletableFuture.supplyAsync( () -> emptyStream, this.getReadingExecutor() )
                                     .thenApply( timeSeries -> ingester.ingest( timeSeries, source ) );

            return Collections.singletonList( task );
        }

        // Create a read/ingest pipeline. The ingester pulls from the reader by advancing the stream
        CompletableFuture<List<IngestResult>> task =
                CompletableFuture.supplyAsync( () -> reader.read( source ), this.getReadingExecutor() )
                                 .thenApply( timeSeries -> ingester.ingest( timeSeries, source ) );
        return Collections.singletonList( task );
    }

    /**
     * @param source the gridded netcdf source to read
     */

    private Supplier<List<IngestResult>> ingestGriddedFeatures( DataSource source )
    {
        return () -> {
            LOGGER.debug( "Checking gridded features for {} to determine whether loading is necessary.",
                          source.getUri() );

            try ( NetcdfFile ncf = NetcdfFiles.open( source.getUri().toString() ) )
            {
                this.getCaches()
                    .getFeaturesCache()
                    .addGriddedFeatures( ncf );
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
     * Determines whether or not a file-like data source should be ingested. Archived data will always be further 
     * evaluated to determine whether its individual entries warrant an ingest
     * 
     * @param dataSource The data source to check
     * @return Whether or not data within the file should be ingested
     * @throws PreIngestException when hashing or id lookup cause some exception
     */
    private FileEvaluation shouldIngestFileSource( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        DataDisposition disposition = dataSource.getDisposition();

        // Archives should always be further evaluated as they are decomposed prior to ingest
        if ( disposition == DataDisposition.GZIP || disposition == DataDisposition.TARBALL )
        {
            LOGGER.debug( "The source at '{}' was detected as an archive format whose contents will need to be further "
                          + "evaluated.",
                          dataSource.getUri() );
            return new FileEvaluation( SourceStatus.NOT_COMPLETE,
                                       KEY_NOT_FOUND );
        }

        // Is this in-memory, i.e., no ingest required?
        if ( this.getSystemSettings()
                 .isInMemory() )
        {
            return new FileEvaluation( SourceStatus.NOT_COMPLETE,
                                       KEY_NOT_FOUND );
        }

        boolean ingest = ( disposition != DataDisposition.UNKNOWN );
        SourceStatus sourceStatus = null;

        // Only require for gridded NetCDF. Remove this when gridded evaluation and vector evaluations are par. See 
        // #51232. 
        String sourceHash = null;

        if ( ingest )
        {
            try
            {
                // If the format is gridded NetCDF, we want to possibly bypass traditional hashing
                if ( disposition == DataDisposition.NETCDF_GRIDDED )
                {
                    sourceHash = NetCDF.getUniqueIdentifier( dataSource.getUri(),
                                                             dataSource.getVariable()
                                                                       .getValue() );
                }

                sourceStatus = this.querySourceStatus( sourceHash );

                // Added in debugging #58715-116
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Determined that a source with URI {} and hash {} has the status of {}.",
                                  dataSource.getUri(),
                                  sourceHash,
                                  sourceStatus );
                }
            }
            catch ( IOException ioe )
            {
                throw new PreIngestException( "Could not determine whether to ingest '"
                                              + dataSource.getUri()
                                              + "'",
                                              ioe );
            }
        }
        else
        {
            LOGGER.debug( "The file at '{}' will not be ingested because it has data that could not be detected as "
                          + "readable by WRES.",
                          dataSource.getUri() );
            return new FileEvaluation( SourceStatus.INVALID,
                                       KEY_NOT_FOUND );
        }

        if ( sourceStatus == null )
        {
            throw new IllegalStateException( "Expected the source status to be set by now." );
        }

        // Get the surrogate key if it exists
        long surrogateKey = this.getSurrogateKey( sourceHash, dataSource.getUri() );

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
     * Determines if another task is responsible for source ingest.
     * @param hash The hash of the file that might need to be ingested
     * @return Whether or not another task has claimed responsibility for data
     * @throws SQLException Thrown if communication with the database failed in
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
        SourceLoader2.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getLeft() );

        // Must have one or more right sources to load and link
        SourceLoader2.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getRight() );

        // May have one or more baseline sources to load and link
        if ( Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
        {
            SourceLoader2.mutateSourcesToLoadAndLink( sources, projectConfig, projectConfig.getInputs().getBaseline() );
        }

        // Create a simple entry (DataSource) for each complex entry
        Set<DataSource> returnMe = new HashSet<>();

        // Expand any file sources that represent directories and filter any that are not required
        for ( Map.Entry<DataSourceConfig.Source, Pair<DataSourceConfig, List<LeftOrRightOrBaseline>>> nextSource : sources.entrySet() )
        {
            // Allow GC of new empty sets by letting the links ref empty set.
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

            // Evaluate the path, which is null for a source that is not file-like
            Path path = SourceLoader2.evaluatePath( systemSettings, nextSource.getKey() );

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

                Set<DataSource> filesources = SourceLoader2.decomposeFileSource( source );
                returnMe.addAll( filesources );
            }
            // Not a file-like source
            else
            {
                // Create a source with unknown disposition as the basis for detection
                DataSource sourceToEvaluate = DataSource.of( DataDisposition.UNKNOWN,
                                                             nextSource.getKey(),
                                                             dataSourceConfig,
                                                             links,
                                                             nextSource.getKey()
                                                                       .getValue(),
                                                             lrb );

                DataDisposition disposition = SourceLoader2.getDispositionOfNonFileSource( sourceToEvaluate );
                DataSource evaluatedSource = DataSource.of( disposition,
                                                            nextSource.getKey(),
                                                            dataSourceConfig,
                                                            links,
                                                            nextSource.getKey()
                                                                      .getValue(),
                                                            lrb );

                returnMe.add( evaluatedSource );
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
            LOGGER.debug( "Identified a source as a web source: {}. Inspecting further...", dataSource );
            if ( ReaderUtilities.isUsgsSource( dataSource ) )
            {
                LOGGER.debug( "Identified a source as a USGS source: {}.", dataSource );
                return DataDisposition.JSON_WATERML;
            }
            else if ( ReaderUtilities.isWrdsAhpsSource( dataSource )
                      || ReaderUtilities.isWrdsObservedSource( dataSource ) )
            {
                LOGGER.debug( "Identified a source as a JSON WRDS AHPS source: {}.", dataSource );
                return DataDisposition.JSON_WRDS_AHPS;
            }
            else if ( ReaderUtilities.isWrdsNwmSource( dataSource ) )
            {
                LOGGER.debug( "Identified a source as a JSON WRDS NWM source: {}.", dataSource );
                return DataDisposition.JSON_WRDS_NWM;
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

        // Directory: must decompose into file sources whose format(s) must be detected
        if ( file.isDirectory() )
        {
            return SourceLoader2.decomposeDirectorySource( dataSource );
        }
        // Regular file, detect the format and return
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
     * @return the source status
     * @throws PreIngestException When communication with the database fails.
     */

    private SourceStatus querySourceStatus( String hash )
    {
        if ( Objects.isNull( hash ) )
        {
            // When the hash is null, report it as not started, etc.
            // As of 2020-06-02, TimeSeriesIngester will investigate status,
            // so do not bother checking here for files.
            return SourceStatus.NOT_COMPLETE;
        }

        try
        {
            boolean anotherTaskStartedIngest = this.anotherTaskIsResponsibleForSource( hash );

            if ( anotherTaskStartedIngest )
            {
                boolean ingestMarkedComplete = this.wasSourceCompleted( hash );

                if ( ingestMarkedComplete )
                {
                    return SourceStatus.COMPLETED;
                }
                else
                {
                    return SourceStatus.NOT_COMPLETE;
                }
            }
            else
            {
                return SourceStatus.NOT_COMPLETE;
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

    private ExecutorService getReadingExecutor()
    {
        return this.readingExecutor;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    private Caches getCaches()
    {
        return this.caches;
    }

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }

    /**
     * @return the reader factory
     */

    private TimeSeriesReaderFactory getTimeSeriesReaderFactory()
    {
        return this.timeSeriesReaderFactory;
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
}
