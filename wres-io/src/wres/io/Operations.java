package wres.io;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.GriddedFeatures;
import wres.io.database.Database;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.SourceLoader;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.project.Projects;
import wres.io.project.InMemoryProject;
import wres.io.project.Project;
import wres.system.SystemSettings;

/**
 * Helpers to conduct ingest and prepare for operations on time-series data.
 */

public final class Operations
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Operations.class );

    /**
     * Ingests time-series data and returns the ingest results.
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings the system settings
     * @param projectConfig the projectConfig for the evaluation
     * @param griddedFeatures the gridded features cache to populate
     * @return the ingest results
     * @throws NullPointerException if any required input is null
     * @throws IngestException when anything else goes wrong
     */

    public static List<IngestResult> ingest( TimeSeriesIngester timeSeriesIngester,
                                             SystemSettings systemSettings,
                                             ProjectConfig projectConfig,
                                             GriddedFeatures.Builder griddedFeatures )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( timeSeriesIngester );

        // Create a thread factory for reading. Inner readers may create additional thread factories (e.g., archives).
        ThreadFactory threadFactoryWithNaming =
                new BasicThreadFactory.Builder().namingPattern( "Outer Reading Thread %d" )
                                                .build();
        ThreadPoolExecutor readingExecutor =
                new ThreadPoolExecutor( systemSettings.getMaximumReadThreads(),
                                        systemSettings.getMaximumReadThreads(),
                                        systemSettings.poolObjectLifespan(),
                                        TimeUnit.MILLISECONDS,
                                        // Queue should be large enough to allow
                                        // join() call below to be reached with
                                        // zero or few rejected submissions to
                                        // the executor service.
                                        new ArrayBlockingQueue<>( 100_000 ),
                                        threadFactoryWithNaming );
        readingExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        List<IngestResult> projectSources = new ArrayList<>();

        SourceLoader loader = new SourceLoader( timeSeriesIngester,
                                                systemSettings,
                                                readingExecutor,
                                                projectConfig,
                                                griddedFeatures );

        try
        {
            Instant start = Instant.now();

            List<CompletableFuture<List<IngestResult>>> ingestions = loader.load();

            // If the count of the list above exceeds the queue in the
            // ExecutorService above, then this Thread will be stuck helping
            // start ingest tasks until the service can catch up. The downside
            // is a delay in exception propagation in some circumstances.
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "{} direct ingest results.", ingestions.size() );
            }

            // Give exception on any of these ingests a chance to propagate fast
            Operations.doAllOrException( ingestions )
                      .join();

            // The loading happened above during join(), now read the results.
            for ( CompletableFuture<List<IngestResult>> task : ingestions )
            {
                List<IngestResult> ingested = task.get();
                projectSources.addAll( ingested );
            }

            Instant stop = Instant.now();

            LOGGER.info( "Finished loading the declared datasets in {}.", Duration.between( start, stop ) );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted during ingest.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( CompletionException | IOException | ExecutionException e )
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
        if ( projectSources.stream().anyMatch( IngestResult::requiresRetry ) )
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
     * Creates an {@link Project} backed by a database.
     * @param database The database to use
     * @param projectConfig the projectConfig to ingest
     * @param caches the database caches/ORMs
     * @param griddedFeatures the gridded features cache, if required
     * @param ingestResults the ingest results
     * @return the project
     * @throws IllegalStateException when another process already holds lock
     * @throws NullPointerException if any input is null
     * @throws IngestException when anything else goes wrong
     */
    public static Project getProject( Database database,
                                      ProjectConfig projectConfig,
                                      DatabaseCaches caches,
                                      GriddedFeatures griddedFeatures,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( caches );
        Objects.requireNonNull( ingestResults );

        try
        {
            return Projects.getProjectFromIngest( database,
                                                  caches,
                                                  griddedFeatures,
                                                  projectConfig,
                                                  ingestResults );
        }
        catch ( SQLException | IngestException | PreIngestException e )
        {
            throw new IngestException( "Failed to finalize ingest.", e );
        }
    }

    /**
     * Creates an {@link Project} backed by an in-memory {@link TimeSeriesStore}.
     * @param projectConfig the projectConfig
     * @param timeSeriesStore the store of time-series data
     * @param ingestResults the ingest results
     * @return the project
     */
    public static Project getProject( ProjectConfig projectConfig,
                                      TimeSeriesStore timeSeriesStore,
                                      List<IngestResult> ingestResults )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( timeSeriesStore );
        Objects.requireNonNull( ingestResults );

        return new InMemoryProject( projectConfig, timeSeriesStore, ingestResults );
    }

    /**
     * Gracefully shuts down all IO operations
     * @param database The database to use.
     */
    public static void shutdown( Database database )
    {
        LOGGER.info( "Shutting down the IO layer..." );
        database.shutdown();
    }

    /**
     * Forces the IO to shutdown all IO operations without finishing stored tasks
     * @param database The database to use.
     * @param timeOut The amount of time to wait while shutting down tasks
     * @param timeUnit The unit with which to measure the shutdown
     */
    public static void forceShutdown( Database database,
                                      long timeOut,
                                      TimeUnit timeUnit )
    {
        LOGGER.info( "Forcefully shutting down the IO module..." );

        List<Runnable> databaseTasks =
                database.forceShutdown( timeOut / 2, timeUnit );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "IO module was forcefully shut down. "
                         + "Abandoned around {} database tasks.",
                         databaseTasks.size() );
        }
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

    private Operations()
    {
    }
}
