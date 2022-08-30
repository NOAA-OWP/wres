package wres.io;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.time.TimeSeriesStore;
import wres.io.concurrency.Executor;
import wres.io.concurrency.Pipelines;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.GriddedFeatures;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.SourceLoader;
import wres.io.ingesting.SourceLoader2;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.project.Projects;
import wres.io.removal.IncompleteIngest;
import wres.io.project.InMemoryProject;
import wres.io.project.Project;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.io.writing.netcdf.NetCDFCopier;
import wres.system.SystemSettings;

/**
 * Helpers to conduct ingest and prepare for operations on time-series data.
 */

public final class Operations
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Operations.class );

    /**
     * Ingests for an evaluation project that requires a database and returns the ingest results.
     * 
     * TODO: remove the lock manager once migrated to {@link SourceLoader2} because this is part of the ingester 
     * implementation. Eventually, remove the database and database ORM too.
     * 
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings the system settings
     * @param database the database
     * @param executor the executor
     * @param projectConfig the projectConfig for the evaluation
     * @param lockManager The lock manager to use.
     * @param caches the database caches/ORMs
     * @param griddedFeatures the gridded features cache to populate
     * @return the ingest results
     * @throws NullPointerException if any required input is null
     * @throws IllegalStateException when another process already holds lock
     * @throws IngestException when anything else goes wrong
     */

    public static List<IngestResult> ingest( TimeSeriesIngester timeSeriesIngester,
                                             SystemSettings systemSettings,
                                             Database database,
                                             Executor executor,
                                             ProjectConfig projectConfig,
                                             DatabaseLockManager lockManager,
                                             DatabaseCaches caches,
                                             GriddedFeatures.Builder griddedFeatures )
    {
        return Operations.doIngestWork( timeSeriesIngester,
                                        systemSettings,
                                        database,
                                        executor,
                                        projectConfig,
                                        lockManager,
                                        caches,
                                        griddedFeatures );
    }

    /**
     * Ingests for an evaluation project that is running in-memory and returns the ingest results.
     * @param timeSeriesIngester The time-series ingester
     * @param systemSettings The system settings
     * @param executor The executor
     * @param projectConfig the projectConfig for the evaluation
     * @param griddedFeatures the gridded features cache to populate
     * @return the ingest results
     * @throws NullPointerException if any required input is null
     * @throws IngestException when anything else goes wrong
     */

    public static List<IngestResult> ingest( TimeSeriesIngester timeSeriesIngester,
                                             SystemSettings systemSettings,
                                             Executor executor,
                                             ProjectConfig projectConfig,
                                             GriddedFeatures.Builder griddedFeatures )
    {
        return Operations.doIngestWork( timeSeriesIngester,
                                        systemSettings,
                                        null,
                                        executor,
                                        projectConfig,
                                        null,
                                        null,
                                        griddedFeatures );
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
     * Ingests and returns the hashes of source files involved in this project.
     * @param timeSeriesIngester The time-series ingester
     * @param systemSettings The system settings
     * @param database The database to use
     * @param executor The executor to use
     * @param projectConfig the projectConfig to ingest
     * @param lockManager The lock manager to use.
     * @param caches the database caches/ORMs
     * @param griddedFeatures the gridded features cache to populate, if required
     * @return the ingest results
     * @throws IllegalStateException when another process already holds lock
     * @throws NullPointerException if any required input is null
     * @throws IngestException when anything else goes wrong
     */
    private static List<IngestResult> doIngestWork( TimeSeriesIngester timeSeriesIngester,
                                                    SystemSettings systemSettings,
                                                    Database database,
                                                    Executor executor,
                                                    ProjectConfig projectConfig,
                                                    DatabaseLockManager lockManager,
                                                    DatabaseCaches caches,
                                                    GriddedFeatures.Builder griddedFeatures )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( executor );
        Objects.requireNonNull( timeSeriesIngester );

        if ( !systemSettings.isInMemory() )
        {
            Objects.requireNonNull( database );
            Objects.requireNonNull( caches );
            Objects.requireNonNull( lockManager );
        }

        ThreadFactory threadFactoryWithNaming =
                new BasicThreadFactory.Builder().namingPattern( "Outer Reading Thread %d" )
                                                .build();
        ThreadPoolExecutor readingExecutor =
                new ThreadPoolExecutor( systemSettings.maximumThreadCount(),
                                        systemSettings.maximumThreadCount(),
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
                                                database,
                                                caches,
                                                projectConfig,
                                                lockManager,
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
            Pipelines.doAllOrException( ingestions )
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
            if ( !systemSettings.isInMemory() )
            {
                List<IngestResult> leftovers = database.completeAllIngestTasks();
                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "{} indirect ingest results.", leftovers.size() );
                }
                projectSources.addAll( leftovers );
            }

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
                                                           .collect( Collectors.toList() );

        List<IngestResult> safeToShareResults =
                Collections.unmodifiableList( composedResults );

        if ( safeToShareResults.isEmpty() )
        {
            throw new IngestException( "No data were ingested." );
        }

        return safeToShareResults;
    }

    /**
     * Gracefully shuts down all IO operations
     * @param database The database to use.
     * @param executor The executor to use.
     */
    public static void shutdown( Database database,
                                 Executor executor )
    {
        LOGGER.info( "Shutting down the IO layer..." );
        executor.complete();
        database.shutdown();
    }

    /**
     * Forces the IO to shutdown all IO operations without finishing stored tasks
     * @param database The database to use.
     * @param executor The executor to use.
     * @param timeOut The amount of time to wait while shutting down tasks
     * @param timeUnit The unit with which to measure the shutdown
     */
    public static void forceShutdown( Database database,
                                      Executor executor,
                                      long timeOut,
                                      TimeUnit timeUnit )
    {
        LOGGER.info( "Forcefully shutting down the IO module..." );

        List<Runnable> executorTasks =
                executor.forceShutdown( timeOut / 2, timeUnit );
        List<Runnable> databaseTasks =
                database.forceShutdown( timeOut / 2, timeUnit );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "IO module was forcefully shut down. "
                         + "Abandoned around {} executor tasks and "
                         + "around {} database tasks.",
                         executorTasks.size(),
                         databaseTasks.size() );
        }
    }

    /**
     * Tests whether or not the WRES may access the database and logs the
     * version of the database it may access
     * @param database The database to use.
     * @throws SQLException when WRES cannot access the database
     */
    public static void testConnection( Database database ) throws SQLException
    {
        try
        {
            final DataScripter script = new DataScripter( database,
                                                          "SELECT 1 as test" );
            final Object result = script.retrieve( "test" );
            LOGGER.info( "Result={}", result );
            LOGGER.info( "Successfully connected to the database" );
        }
        catch ( final SQLException e )
        {
            throw new SQLException( "Could not connect to the database.", e );
        }
    }


    /**
     * Removes all loaded user information from the database
     * Assumes that the caller has already gotten an exclusive lock for modify.
     * @param database The database to use.
     * @throws SQLException when cleaning or refreshing stats fails
     */

    public static void cleanDatabase( Database database ) throws SQLException
    {
        database.clean();
        database.refreshStatistics( true );
    }

    /**
     * Updates the statistics and removes all dead rows from the database
     * Assumes caller has already obtained exclusive lock on database.
     * @param database The database to use.
     * @throws SQLException if the orphaned data could not be removed or the refreshing of statistics fails
     */
    public static void refreshDatabase( Database database ) throws SQLException
    {
        IncompleteIngest.removeOrphanedData( database );
        database.refreshStatistics( true );
    }

    public static void createNetCDFOutputTemplate( final String sourceName, final String templateName )
            throws IOException
    {
        try ( NetCDFCopier writer = new NetCDFCopier( sourceName, templateName, ZonedDateTime.now() ) )
        {
            writer.write();
        }
    }

    private Operations()
    {
    }
}
