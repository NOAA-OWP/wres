package wres.io.ingesting;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
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
import wres.io.data.caching.GriddedFeatures;
import wres.io.reading.DataSource;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.reading.ReadException;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.TimeSeriesReaderFactory;
import wres.io.reading.TimeSeriesTuple;
import wres.system.SystemSettings;

/**
 * <p>This is where reading of time-series formats meets ingesting of time-series (e.g., into a persistent store). Creates
 * one or more {@link DataSource} for reading and ingesting and then reads and ingests them. Reading in this context 
 * means instantiating a {@link TimeSeriesReader} and supplying the reader with a {@link DataSource} to read, while 
 * ingesting means passing the resulting time-series and its descriptive {@link DataSource} to a 
 * {@link TimeSeriesIngester}, which is supplied on construction. Also handles other tasks between reading and ingest, 
 * such as the application of a consistent missing value identifier (for missing values that are declared, rather than 
 * integral to readers).
 *
 * <p>TODO: Remove the gridded features cache once #51232 is addressed.
 *
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */
public class SourceLoader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SourceLoader.class );

    /** Pre-ingest operations. */
    private static final MissingValueOperator MISSING_VALUE_OPERATOR = MissingValueOperator.of();

    /** Thread pool executor for reading time-series formats. */
    private final ExecutorService readingExecutor;

    /** Gridded feature cache. See #51232. */
    private final GriddedFeatures.Builder griddedFeatures;

    /** System settings. */
    private final SystemSettings systemSettings;

    /** Time-series ingester. **/
    private final TimeSeriesIngester timeSeriesIngester;

    /** Factory for creating time-series format readers. **/
    private final TimeSeriesReaderFactory timeSeriesReaderFactory;

    /** The project declaration. */
    private final ProjectConfig projectConfig;

    /**
     * @param timeSeriesIngester the time-series ingester, required
     * @param systemSettings the system settings, required
     * @param readingExecutor the executor for reading, required
     * @param projectConfig the project declaration, required along with the pair element
     * @param griddedFeatures the gridded features cache to populate, only required for a gridded evaluation
     * @throws NullPointerException if any required input is null
     */

    public SourceLoader( TimeSeriesIngester timeSeriesIngester,
                         SystemSettings systemSettings,
                         ExecutorService readingExecutor,
                         ProjectConfig projectConfig,
                         GriddedFeatures.Builder griddedFeatures )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( readingExecutor );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getPair() );

        this.systemSettings = systemSettings;
        this.readingExecutor = readingExecutor;
        this.projectConfig = projectConfig;
        this.timeSeriesIngester = timeSeriesIngester;
        this.griddedFeatures = griddedFeatures;
        this.timeSeriesReaderFactory = TimeSeriesReaderFactory.of( projectConfig.getPair(),
                                                                   systemSettings,
                                                                   griddedFeatures );
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
        Set<DataSource> sources = SourceLoader.createSourcesToLoadAndLink( this.getSystemSettings(),
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

        // Ingest gridded metadata/features when required. For an in-memory evaluation, it is required by the gridded
        // reader. For a database evaluation, it is required by the DatabaseProject. This is a special snowflake until 
        // #51232 is resolved
        if ( source.getDisposition() == DataDisposition.NETCDF_GRIDDED )
        {
            Supplier<List<IngestResult>> fakeResult = this.ingestGriddedFeatures( source );
            CompletableFuture<List<IngestResult>> future = CompletableFuture.supplyAsync( fakeResult,
                                                                                          this.getReadingExecutor() );
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

        List<CompletableFuture<List<IngestResult>>> futureList = this.readAndIngestData( source );
        List<CompletableFuture<List<IngestResult>>> tasks = new ArrayList<>( futureList );

        return Collections.unmodifiableList( tasks );
    }

    /**
     * <p>Loads a web-like source.
     *
     * <p>TODO: create links for a non-file source when it appears in more than one context, i.e.
     * {@link LeftOrRightOrBaseline}.
     *
     * <p>See {@link #loadFileSource(DataSource)} for how this is done with a file source.
     *
     * <p>See #67774.
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

        TimeSeriesIngester ingester = this.getTimeSeriesIngester();

        // As of 20220824, grids are not read at "read" time unless there is an in-memory evaluation. See #51232.
        // Until this special snowflake is addressed via #51232, there is no missing value mapping for declared missing
        // values beyond any mapping that is performed by the reader using inband missing value identifiers: #88859
        if ( source.isGridded() && this.getSystemSettings()
                                       .isInDatabase() )
        {
            // Empty stream, which will trigger source ingest only, not time-series reading/ingest
            Stream<TimeSeriesTuple> emptyStream = Stream.of();

            CompletableFuture<List<IngestResult>> task =
                    CompletableFuture.supplyAsync( () -> emptyStream, this.getReadingExecutor() )
                                     .thenApply( timeSeries -> ingester.ingest( timeSeries, source ) );

            return Collections.singletonList( task );
        }

        TimeSeriesReader reader = this.getTimeSeriesReaderFactory()
                                      .getReader( source );

        // Create a read/ingest pipeline. The ingester pulls from the reader by advancing the stream
        CompletableFuture<List<IngestResult>> task =
                CompletableFuture.supplyAsync( () -> reader.read( source ), this.getReadingExecutor() )
                                 // Map any declared missing values to a common missing value identifier
                                 .thenApply( MISSING_VALUE_OPERATOR )
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
     * <p>Evaluates a project and creates a {@link DataSource} for each distinct source within the project that needs to
     * be loaded, together with any additional links required. A link is required for each additional context, i.e. 
     * {@link LeftOrRightOrBaseline}, in which the source appears. The links are returned by 
     * {@link DataSource#getLinks()}. Here, a "link" means a separate entry in <code>wres.ProjectSource</code>.
     *
     * <p>A {@link DataSource} is returned for each discrete source. When the declared {@link DataSourceConfig.Source}
     * points to a directory of files, the tree is walked and a {@link DataSource} is returned for each one within the 
     * tree that meets any prescribed filters.
     *
     * @param systemSettings the system settings
     * @param projectConfig the project declaration
     * @return the set of distinct sources to load and any additional links to create
     */

    private static Set<DataSource> createSourcesToLoadAndLink( SystemSettings systemSettings,
                                                               ProjectConfig projectConfig )
    {
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
            Path path = SourceLoader.evaluatePath( systemSettings, nextSource.getKey() );

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
                DataSource sourceToEvaluate = DataSource.of( DataDisposition.UNKNOWN,
                                                             nextSource.getKey(),
                                                             dataSourceConfig,
                                                             links,
                                                             nextSource.getKey()
                                                                       .getValue(),
                                                             lrb );

                DataDisposition disposition = SourceLoader.getDispositionOfNonFileSource( sourceToEvaluate );
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
     * @return the disposition
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
            // Hosted NWM data, not via a WRDS API
            else if ( ReaderUtilities.isNwmVectorSource( dataSource ) )
            {
                return DataDisposition.NETCDF_VECTOR;
            }

            String append = "";

            if ( Objects.isNull( dataSource.getSource()
                                           .getInterface() ) )
            {
                append = " Please declare the interface for the source and try again.";
            }

            throw new UnsupportedOperationException( "Discovered an unsupported data source: "
                                                     + dataSource
                                                     + ". The data source was identified as a web source, but its "
                                                     + "disposition could not be discovered."
                                                     + append );
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
            return SourceLoader.decomposeDirectorySource( dataSource );
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

        if ( !( pattern == null || pattern.isEmpty() ) )
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

        // Is there a source path to evaluate? Only if the source is file-like
        if ( uri.toString()
                .isEmpty() )
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
     * @return the system settings
     */

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    /**
     * @return the reading executor
     */

    private ExecutorService getReadingExecutor()
    {
        return this.readingExecutor;
    }

    /**
     * @return the gridded features cache
     * @throws NullPointerException if the gridded features are undefined
     */

    private GriddedFeatures.Builder getGriddedFeatures()
    {
        Objects.requireNonNull( this.griddedFeatures,
                                "Cannot read a gridded dataset without a gridded features cache." );

        return this.griddedFeatures;
    }

    /**
     * @return the project declaration
     */

    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }

    /**
     * @return the time-series ingester
     */

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

}
