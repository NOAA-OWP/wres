package wres.io.ingesting;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;

import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationUtilities;
import wres.config.yaml.components.CovariateDataset;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Source;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.reading.DataSource;
import wres.reading.ReaderUtilities;
import wres.reading.DataSource.DataDisposition;
import wres.reading.ReadException;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesReaderFactory;
import wres.reading.TimeSeriesTuple;
import wres.system.SystemSettings;

/**
 * <p>This is where reading of time-series formats meets ingesting of time-series. In this context, ingesting could
 * mean persisting the time-series data in a database or simply forwarding them to an in-memory store. The two
 * pipeline activities of reading and ingesting are collectively referred to as "loading" sources. Creates one or
 * more {@link DataSource} for reading and ingesting and then reads and ingests them. Reading in this context means
 * instantiating a {@link TimeSeriesReader} and supplying the reader with a {@link DataSource} to read, while
 * ingesting means passing the resulting time-series and its descriptive {@link DataSource} to a
 * {@link TimeSeriesIngester}, which is supplied on construction. Also handles other tasks between reading and
 * ingest, such as the application of a consistent missing value identifier (for missing values that are declared,
 * rather than integral to readers).
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
    private final EvaluationDeclaration declaration;

    /**
     * Reads and ingests time-series data and returns ingest results.
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings the system settings
     * @param declaration the projectConfig for the evaluation
     * @param griddedFeatures the gridded features cache to populate
     * @param readingExecutor the executor for performing reading tasks
     * @return ingest results
     * @throws NullPointerException if any required input is null
     * @throws ReadException if the source could not be read
     * @throws IngestException if the time-series could not be ingested
     */

    public static List<IngestResult> load( TimeSeriesIngester timeSeriesIngester,
                                           SystemSettings systemSettings,
                                           EvaluationDeclaration declaration,
                                           GriddedFeatures.Builder griddedFeatures,
                                           ExecutorService readingExecutor )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( readingExecutor );

        List<IngestResult> projectSources = new ArrayList<>();

        SourceLoader loader = new SourceLoader( timeSeriesIngester,
                                                systemSettings,
                                                readingExecutor,
                                                declaration,
                                                griddedFeatures );

        try
        {
            Instant start = Instant.now();

            List<CompletableFuture<List<IngestResult>>> ingested = loader.load();

            // If the count of the list above exceeds the queue in the
            // ExecutorService above, then this Thread will be stuck helping
            // start ingest tasks until the service can catch up. The downside
            // is a delay in exception propagation in some circumstances.
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "{} direct ingest results.", ingested.size() );
            }

            // Give exception on any of these ingests a chance to propagate fast
            SourceLoader.doAllOrException( ingested )
                        .join();

            // The loading happened above during join(), now read the results.
            for ( CompletableFuture<List<IngestResult>> task : ingested )
            {
                List<IngestResult> ingestResults = task.get();
                projectSources.addAll( ingestResults );
            }

            Instant stop = Instant.now();

            LOGGER.info( "Finished loading the declared datasets in {}.", Duration.between( start, stop ) );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted during ingest.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( CompletionException | ExecutionException e )
        {
            String message = "An ingest task could not be completed.";
            throw new IngestException( message, e );
        }
        finally
        {
            // Close the ingest executor
            readingExecutor.shutdownNow();
        }

        LOGGER.debug( "Here are the files ingested: {}", projectSources );

        // Are there any sources that need to be retried? If so, that is exceptional, because retries happen in-band to
        // ingest. In practice, this scenario is unlikely because ingest should throw an exception once all retries are
        // exhausted: #89229
        if ( projectSources.stream()
                           .anyMatch( IngestResult::requiresRetry ) )
        {
            throw new IngestException( "Discovered one or more time-series that had not been ingested after all "
                                       + "retries were exhausted." );
        }

        List<IngestResult> composedResults = projectSources.stream()
                                                           .toList();

        if ( composedResults.isEmpty() )
        {
            throw new IngestException( "No data were ingested." );
        }

        return composedResults;
    }

    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     *
     * @param <T> the type of future
     * @param futures the futures to compose
     * @return the composed futures
     * @throws CompletionException if completing exceptionally
     */

    static <T> CompletableFuture<Object> doAllOrException( final List<CompletableFuture<T>> futures )
    {
        //Complete when all futures are completed
        final CompletableFuture<Void> allDone =
                CompletableFuture.allOf( futures.toArray( new CompletableFuture[0] ) );
        //Complete when any of the underlying futures completes exceptionally
        final CompletableFuture<T> oneExceptional = new CompletableFuture<>();
        //Link the two
        for ( final CompletableFuture<T> completableFuture : futures )
        {
            //When one completes exceptionally, propagate
            completableFuture.exceptionally( exception -> {
                oneExceptional.completeExceptionally( exception );
                return null;
            } );
        }
        //Either all done OR one completes exceptionally
        return CompletableFuture.anyOf( allDone, oneExceptional );
    }

    /**
     * Loads the time-series data.
     *
     * @return List of future ingest results
     * @throws IngestException when loading fails
     */

    private List<CompletableFuture<List<IngestResult>>> load()
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
        Set<DataSource> sources = SourceLoader.createSourcesToLoadAndLink( this.getDeclaration() );

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
            // It has now been established that a gridded evaluation is required. Gridded evaluations require a spatial
            // mask to determine the features to evaluate. Check that now.
            if ( Objects.isNull( this.getDeclaration()
                                     .spatialMask() ) )
            {
                throw new DeclarationException( "Discovered gridded data, but the declaration does not include a "
                                                + "'spatial_mask'. Gridded evaluations with an unbounded selection "
                                                + "are not currently supported. Please add a 'spatial_mask' to the "
                                                + "declaration and try again." );
            }

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
                throw new ReadException( "Could not read the gridded features.", e );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread()
                      .interrupt();

                throw new ReadException( "Could not read the gridded features.", e );
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
     * {@link DatasetOrientation}.
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

        // As of 20220824, grids are not read at "read" time unless there is an in-memory evaluation. Instead, they are
        // read at "retrieval" time using an appropriate implementation of a TimeSeriesRetriever. See #51232.
        // Until this special snowflake is addressed via #51232, there is no missing value mapping for declared missing
        // values beyond any mapping that is performed by the reader using inband missing value identifiers: #88859
        if ( source.isGridded()
             && this.getSystemSettings()
                    .isUseDatabase() )
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
     * <p>Evaluates a project and creates a {@link DataSource} for each distinct source within the project that needs
     * to be loaded, together with any additional links required. A link is required for each additional context, i.e.
     * {@link DatasetOrientation}, in which the source appears. The links are returned by
     * {@link DataSource#getLinks()}. Here, a "link" means a separate entry in <code>wres.ProjectSource</code>.
     *
     * <p>A {@link DataSource} is returned for each discrete source. When the declared {@link Source} points to a
     * directory of files, the tree is walked and a {@link DataSource} is returned for each one within the tree that
     * meets any prescribed filters.
     *
     * @param declaration the project declaration
     * @return the set of distinct sources to load and any additional links to create
     */

    private static Set<DataSource> createSourcesToLoadAndLink( EvaluationDeclaration declaration )
    {
        // Somewhat convoluted structure that will be turned into a simple one.
        // The key is the distinct source, and the paired value is the context in
        // which the source appears and the set of additional links to create, if any.
        // Note that all project declaration overrides hashCode and equals (~ key in a HashMap)
        Map<Source, Pair<OrientedDataSource, List<DatasetOrientation>>> sources = new HashMap<>();

        // Must have one or more left sources to load and link
        SourceLoader.mutateSourcesToLoadAndLink( sources,
                                                 declaration,
                                                 declaration.left(),
                                                 DatasetOrientation.LEFT,
                                                 null );

        // Must have one or more right sources to load and link
        SourceLoader.mutateSourcesToLoadAndLink( sources,
                                                 declaration,
                                                 declaration.right(),
                                                 DatasetOrientation.RIGHT,
                                                 null );

        // May have one or more baseline sources to load and link
        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            SourceLoader.mutateSourcesToLoadAndLink( sources,
                                                     declaration,
                                                     declaration.baseline()
                                                                .dataset(),
                                                     DatasetOrientation.BASELINE,
                                                     null );
        }

        // Prepare the covariate datasets
        for ( CovariateDataset covariate : declaration.covariates() )
        {
            SourceLoader.mutateSourcesToLoadAndLink( sources,
                                                     declaration,
                                                     covariate.dataset(),
                                                     DatasetOrientation.COVARIATE,
                                                     covariate.featureNameOrientation() );
        }

        // Create a simple entry (DataSource) for each complex entry
        Set<DataSource> returnMe = new HashSet<>();

        // Expand any file sources that represent directories and filter any that are not required
        for ( Map.Entry<Source, Pair<OrientedDataSource, List<DatasetOrientation>>> nextSource : sources.entrySet() )
        {
            // Allow GC of new empty sets by letting the links ref empty set.
            List<DatasetOrientation> links = Collections.emptyList();

            if ( !nextSource.getValue()
                            .getRight()
                            .isEmpty() )
            {
                links = nextSource.getValue()
                                  .getRight();
            }

            OrientedDataSource dataSource = nextSource.getValue()
                                                      .getLeft();
            Dataset dataset = dataSource.dataset();
            DatasetOrientation orientation = dataSource.orientation();
            DatasetOrientation covariateFeatureOrientation = dataSource.covariateFeatureorientation();

            // Evaluate the path, which is null for a source that is not file-like
            Path path = SourceLoader.evaluatePath( nextSource.getKey() );

            // If there is a file-like source, test for a directory and decompose it as required
            if ( Objects.nonNull( path ) )
            {
                // Currently unknown disposition, to be unpacked/determined
                DataSource source = DataSource.of( DataDisposition.UNKNOWN,
                                                   nextSource.getKey(),
                                                   dataset,
                                                   links,
                                                   path.toUri(),
                                                   orientation,
                                                   covariateFeatureOrientation );

                Set<DataSource> filesources = SourceLoader.decomposeFileSource( source );
                returnMe.addAll( filesources );
            }
            // Not a file-like source
            else
            {
                // Create a source with unknown disposition as the basis for detection
                DataSource sourceToEvaluate = DataSource.of( DataDisposition.UNKNOWN,
                                                             nextSource.getKey(),
                                                             dataset,
                                                             links,
                                                             nextSource.getKey()
                                                                       .uri(),
                                                             orientation,
                                                             covariateFeatureOrientation );

                DataDisposition disposition = SourceLoader.getDispositionOfNonFileSource( sourceToEvaluate );
                DataSource evaluatedSource = DataSource.of( disposition,
                                                            nextSource.getKey(),
                                                            dataset,
                                                            links,
                                                            nextSource.getKey()
                                                                      .uri(),
                                                            orientation,
                                                            covariateFeatureOrientation );

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
                                           .sourceInterface() ) )
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
                                                    dataSource.getDatasetOrientation(),
                                                    dataSource.getCovariateFeatureOrientation() );
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
                                                        dataSource.getDatasetOrientation(),
                                                        dataSource.getCovariateFeatureOrientation() );
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

        Source source = dataSource.getSource();

        // Define path matcher based on the source's pattern, if provided.
        PathMatcher matcher;

        String pattern = source.pattern();

        if ( !( pattern == null || pattern.isEmpty() ) )
        {
            matcher = FileSystems.getDefault()
                                 .getPathMatcher( "glob:" + pattern );
        }
        else
        {
            matcher = null;
        }

        // Walk the tree and find sources that match a pattern or none
        List<File> unmatchedByPattern = new ArrayList<>();
        try ( Stream<Path> files = Files.walk( sourcePath ) )
        {
            files.forEach( path -> {

                File testFile = path.toFile();

                // File must be a file and match the pattern, if the pattern is defined.
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
                                                     dataSource.getDatasetOrientation(),
                                                     dataSource.getCovariateFeatureOrientation() ) );
                    }
                    else
                    {
                        LOGGER.warn( "Skipping '{}' because WRES will not be able to parse it.",
                                     path );
                    }
                }
                // Skip and log a warning if this is a normal file (e.g., not a directory)
                else if ( testFile.isFile() && LOGGER.isWarnEnabled() )
                {
                    unmatchedByPattern.add( testFile );
                }
            } );

        }
        catch ( IOException e )
        {
            throw new IngestException( "Failed to walk the directory tree '"
                                       + sourcePath
                                       + "':",
                                       e );
        }

        SourceLoader.logUnmatchedSources( unmatchedByPattern, pattern, dataSource.getDatasetOrientation() );

        //If the results are empty, then there were either no files in the specified source or pattern matched 
        //none of the files.  
        if ( returnMe.isEmpty() )
        {
            throw new IngestException( "Could not find any valid source files within the directory '"
                                       + dataSource.getUri()
                                       + "'. The following pattern filter was used (null if no filter): \""
                                       + pattern
                                       + "\"." );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Logs up to the first one hundred sources unmatched by a source pattern, when required.
     * @param unmatchedByPattern the sources unmatched by a pattern
     * @param pattern the pattern
     * @param orientation the data orientation
     */

    private static void logUnmatchedSources( List<File> unmatchedByPattern,
                                             String pattern,
                                             DatasetOrientation orientation )
    {
        if ( LOGGER.isWarnEnabled() && !unmatchedByPattern.isEmpty() )
        {
            String start = "The skipped sources are: ";
            List<File> logMe = unmatchedByPattern;
            if ( unmatchedByPattern.size() > 100 )
            {
                start = "The first 100 skipped sources are: ";
                logMe = unmatchedByPattern.subList( 0, 100 );
            }

            LOGGER.warn( "Skipping {} '{}' sources because they do not match pattern \"{}\". {}{}",
                         unmatchedByPattern.size(),
                         orientation,
                         pattern,
                         start,
                         logMe );
        }
    }

    /**
     * Evaluate a path from a {@link Source}.
     * @param source the source
     * @return the path of a file-like source or null
     */

    private static Path evaluatePath( Source source )
    {
        LOGGER.trace( "Called evaluatePath with source {}", source );
        URI uri = source.uri();

        // Empty URI, cannot create a path
        if ( uri.toString()
                .isEmpty() )
        {
            LOGGER.debug( "The source value was empty from source {}", source );
            return null;
        }

        // Web source, cannot create a path
        if ( ReaderUtilities.isWebSource( uri ) )
        {
            LOGGER.debug( "Inspected the URI '{}' and discovered a web source.", uri );

            return null;
        }

        // File-like source
        return Path.of( uri );
    }

    /**
     * Mutates the input map of sources, adding additional sources to load or link from the input {@link Dataset}.
     *
     * @param sources the map of sources to mutate
     * @param declaration the project configuration
     * @param dataset the dataset for which sources to load or link are required
     * @param orientation the orientation of the source
     * @param covariateFeatureOrientation the feature orientation of a covariate dataset
     * @throws NullPointerException if any input is null
     */

    private static void mutateSourcesToLoadAndLink( Map<Source, Pair<OrientedDataSource, List<DatasetOrientation>>> sources,
                                                    EvaluationDeclaration declaration,
                                                    Dataset dataset,
                                                    DatasetOrientation orientation,
                                                    DatasetOrientation covariateFeatureOrientation )
    {
        Objects.requireNonNull( sources );
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( dataset );
        Objects.requireNonNull( orientation );

        // Must have one or more right sources
        for ( Source source : dataset.sources() )
        {
            // Link or load?
            // NOTE: there are some paired contexts in which it would be wrong for
            // the source to appear together (e.g. both left and right),
            // but that validation needs to happen way before now, so proceed in all cases

            // Only create a link if the source is already in the load list/map
            if ( sources.containsKey( source ) )
            {
                sources.get( source )
                       .getRight()
                       .add( orientation );
            }
            // Load
            else
            {
                OrientedDataSource orientedSource = new OrientedDataSource( dataset,
                                                                            orientation,
                                                                            covariateFeatureOrientation );
                Pair<OrientedDataSource, List<DatasetOrientation>> sourcePair
                        = Pair.of( orientedSource, new ArrayList<>() );
                sources.put( source, sourcePair );
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

    private EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
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

    /**
     * @param timeSeriesIngester the time-series ingester, required
     * @param systemSettings the system settings, required
     * @param readingExecutor the executor for reading, required
     * @param declaration the project declaration
     * @param griddedFeatures the gridded features cache to populate, only required for a gridded evaluation
     * @throws NullPointerException if any required input is null
     */

    private SourceLoader( TimeSeriesIngester timeSeriesIngester,
                          SystemSettings systemSettings,
                          ExecutorService readingExecutor,
                          EvaluationDeclaration declaration,
                          GriddedFeatures.Builder griddedFeatures )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( readingExecutor );
        Objects.requireNonNull( declaration );

        this.systemSettings = systemSettings;
        this.readingExecutor = readingExecutor;
        this.declaration = declaration;
        this.timeSeriesIngester = timeSeriesIngester;
        this.griddedFeatures = griddedFeatures;
        this.timeSeriesReaderFactory = TimeSeriesReaderFactory.of( declaration,
                                                                   systemSettings,
                                                                   griddedFeatures );
    }

    /**
     * An oriented dataset.
     *
     * @param dataset the dataset
     * @param orientation the orientation
     * @param covariateFeatureorientation the feature orientation of a covariate dataset
     */
    private record OrientedDataSource( Dataset dataset,
                                       DatasetOrientation orientation,
                                       DatasetOrientation covariateFeatureorientation ) {}

}
