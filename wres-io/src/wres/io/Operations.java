package wres.io;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.concurrency.Pipelines;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.project.Projects;
import wres.io.reading.PreIngestException;
import wres.io.removal.IncompleteIngest;
import wres.io.project.Project;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.SourceLoader;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.io.utilities.NoDataException;
import wres.io.writing.netcdf.NetCDFCopier;
import wres.system.SystemSettings;
import wres.util.CalculationException;

public final class Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Operations.class);

    private Operations ()
    {
    }


    /**
     * Prepares IO operations for evaluation execution checks if evaluations
     * are valid/possible
     * @param project The project to evaluate
     * @throws IOException Thrown if the project itself could not be prepared
     * @throws IOException Thrown if the process for determining whether or not
     * variables are valid fails
     * @throws NoDataException Thrown if a variable to evaluate is not
     * accessible to the evaluation
     */

    public static void prepareForExecution( Project project ) throws IOException
    {
        LOGGER.info("Loading preliminary metadata...");

        final String INTERRUPTED_VARIABLE_VALIDATION_MESSAGE =
                "The process for determining if '{}' is a valid variable was interrupted.";

        boolean isVector;
        Variables variables = project.getVariablesCache();
        Executor executor = project.getExecutor();
        Future<Boolean> leftTimeSeriesValid= null;
        Future<Boolean> rightTimeSeriesValid = null;
        Future<Boolean> baselineTimeSeriesValid = null;

        try
        {
            isVector = !( project.usesGriddedData( project.getLeft() ) ||
                          project.usesGriddedData( project.getRight() ));
        }
        catch ( SQLException e )
        {
            throw new IOException("Could not determine if this project uses "
                                  + "gridded data or not.", e);
        }

        if ( isVector )
        {
            leftTimeSeriesValid = executor.submit(
                    () -> variables.isValid( project.getId(),
                                             Project.LEFT_MEMBER,
                                             project.getLeftVariableName()
                    )
            );
        }

        if ( isVector )
        {
            rightTimeSeriesValid = executor.submit(
                    () -> variables.isValid( project.getId(),
                                             Project.RIGHT_MEMBER,
                                             project.getRightVariableName()
                    )
            );
        }

        if ( isVector && project.getBaseline() != null )
        {
            baselineTimeSeriesValid = executor.submit(
                    () -> variables.isValid( project.getId(),
                                             Project.BASELINE_MEMBER,
                                             project.getBaselineVariableName()
                    )
            );
        }

        try
        {
            project.prepareForExecution();
        }
        catch (CalculationException | SQLException exception)
        {
            throw new IOException("This project could not be prepared for "
                                  + "execution.", exception);
        }

        // If we're performing gridded evaluation, we can't check if our
        // variables are valid via normal means, so just return
        if (!isVector)
        {
            LOGGER.info("Preliminary metadata loading is complete.");
            return;
        }

        // TODO: Split logic out into separate functions
        try
        {
            boolean leftIsValid = leftTimeSeriesValid.get();

            if (!leftIsValid)
            {
                List<String> availableVariables = variables.getAvailableVariables(
                        project.getId(),
                        LeftOrRightOrBaseline.LEFT.value()
                );

                StringBuilder message = new StringBuilder(  );
                message.append( "There is no '")
                       .append( project.getLeft().getVariable().getValue())
                       .append("' data available for the left hand data ")
                       .append("evaluation dataset.");

                if (!availableVariables.isEmpty())
                {

                    message.append(" Available variable(s):");
                    for (String variable : availableVariables)
                    {
                        message.append(System.lineSeparator())
                               .append("    ").append(variable);
                    }
                }
                else
                {
                    message.append(" There are no other variables available for use.");
                }

                throw new NoDataException( message.toString() );
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( INTERRUPTED_VARIABLE_VALIDATION_MESSAGE,
                         project.getLeft().getVariable().getValue(), e );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            throw new IOException( "An error occurred while determining whether '"
                                   + project.getLeft().getVariable().getValue()
                                   + "' is a valid variable for left side evaluation.",
                                   e );
        }
        catch ( SQLException e )
        {
            throw new IOException( "'"
                                   + project.getLeft().getVariable().getValue()
                                   + "' is not a valid variable for right hand "
                                   + "evaluation. Possible alternatives could "
                                   + "not be found.", e);
        }

        try
        {
            boolean rightIsValid = rightTimeSeriesValid.get();

            if (!rightIsValid)
            {
                List<String> availableVariables = variables.getAvailableVariables(
                        project.getId(),
                        LeftOrRightOrBaseline.RIGHT.value()
                );

                String message = "There is no '"
                                 + project.getRightVariableName()
                                 + "' data available for the right hand data "
                                 + "evaluation dataset.";

                if (availableVariables.size() > 0)
                {
                    message += " Available variable(s):";
                    for (String variable : availableVariables)
                    {
                        message += System.lineSeparator() + "    " + variable;
                    }
                }
                else
                {
                    message += " There are no other available variables for use.";
                }

                throw new NoDataException( message );
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( INTERRUPTED_VARIABLE_VALIDATION_MESSAGE,
                         project.getRightVariableName(), e );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            throw new IOException( "An error occurred while determining whether '"
                                   + project.getRightVariableName()
                                   + "' is a valid variable for right side evaluation.",
                                   e );
        }
        catch ( SQLException e )
        {
            throw new IOException( "'"
                                   + project.getRightVariableName()
                                   + "' is not a valid variable for right hand "
                                   + "evaluation. Possible alternatives could "
                                   + "not be found.", e);
        }

        // If baselineValid is null, then we have no baseline variable to
        // evaluate; it is safe to exit.
        if ( baselineTimeSeriesValid == null )
        {
            LOGGER.info("Preliminary metadata loading is complete.");
            return;
        }

        try
        {
            boolean baselineIsValid = baselineTimeSeriesValid.get();

            if (!baselineIsValid)
            {
                List<String> availableVariables = variables.getAvailableVariables(
                        project.getId(),
                        LeftOrRightOrBaseline.BASELINE.value()
                );
                String message = "There is no '"
                                 + project.getBaseline().getVariable().getValue()
                                 + "' data available for the baseline "
                                 + "evaluation dataset.";

                if (availableVariables.size() > 0)
                {
                    message += " Available variable(s):";
                    for (String variable : availableVariables)
                    {
                        message += System.lineSeparator() + "    " + variable;
                    }
                }
                else
                {
                    message += " There are no other available variables for use.";
                }

                throw new NoDataException( message );
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( INTERRUPTED_VARIABLE_VALIDATION_MESSAGE,
                         project.getBaseline().getVariable().getValue(), e );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            throw new IOException( "An error occurred while determining whether '"
                                   + project.getBaseline().getVariable().getValue()
                                   + "' is a valid variable for baseline evaluation.",
                                   e );
        }
        catch ( SQLException e )
        {
            throw new IOException( "'"
                                   + project.getBaseline().getVariable().getValue()
                                   + "' is not a valid variable for right hand "
                                   + "evaluation. Possible alternatives could "
                                   + "not be found.", e);
        }
        LOGGER.info("Preliminary metadata loading is complete.");
    }


    /**
     * Ingests for an evaluation project and returns state regarding the same.
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param executor The executor to use.
     * @param projectConfig the projectConfig for the evaluation
     * @param lockManager The lock manager to use.
     * @return the {@link Project} (state about this evaluation)
     * @throws IllegalStateException when another process already holds lock.
     */

    public static Project ingest( SystemSettings systemSettings,
                                  Database database,
                                  Executor executor,
                                  ProjectConfig projectConfig,
                                  DatabaseLockManager lockManager )
    {
        return Operations.doIngestWork( systemSettings, database, executor, projectConfig, lockManager );
    }

    /**
     * Ingests and returns the hashes of source files involved in this project.
     * TODO: Find a more appropriate location; this should call the ingest logic, not implement it
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param executor The executor to use
     * @param projectConfig the projectConfig to ingest
     * @param lockManager The lock manager to use.
     * @return the projectdetails object from ingesting this project
     * @throws IngestException when anything goes wrong
     */
    private static Project doIngestWork( SystemSettings systemSettings,
                                         Database database,
                                         Executor executor,
                                         ProjectConfig projectConfig,
                                         DatabaseLockManager lockManager )
    {
        ThreadFactory threadFactoryWithNaming = new BasicThreadFactory.Builder()
                .namingPattern( "Outer Reading/Ingest Thread %d" )
                .build();
        ThreadPoolExecutor ingestExecutor =
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
        ingestExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        Project result = null;
        List<IngestResult> projectSources = new ArrayList<>();
        DataSources dataSourcesCache = new DataSources( database );
        Features featuresCache = new Features( database );
        Variables variablesCache = new Variables( database );
        Ensembles ensemblesCache = new Ensembles( database );
        MeasurementUnits measurementUnitsCache = new MeasurementUnits( database );

        SourceLoader loader = new SourceLoader( systemSettings,
                                                ingestExecutor,
                                                database,
                                                dataSourcesCache,
                                                featuresCache,
                                                variablesCache,
                                                ensemblesCache,
                                                measurementUnitsCache,
                                                projectConfig,
                                                lockManager );

        try
        {
            List<CompletableFuture<List<IngestResult>>> ingestions = loader.load();

            // If the count of the list above exceeds the queue in the
            // ExecutorService above, then this Thread will be stuck helping
            // start ingest tasks until the service can catch up. The downside
            // is a delay in exception propagation in some circumstances.
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( ingestions.size() + " direct ingest results." );
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
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted during ingest.", ie );
            ingestExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        catch ( CompletionException | IOException | ExecutionException e )
        {
            ingestExecutor.shutdownNow();
            String message = "An ingest task could not be completed.";
            throw new IngestException( message, e );
        }
        finally
        {
            List<IngestResult> leftovers = database.completeAllIngestTasks();
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( leftovers.size() + " indirect ingest results" );
            }
            projectSources.addAll( leftovers );
        }

        LOGGER.debug( "Here are the files ingested: {}", projectSources );

        // Are there any sources that need to be retried?
        List<IngestResult> retriesNeeded = projectSources.stream()
                                                         .filter( IngestResult::requiresRetry )
                                                         .collect( Collectors.toUnmodifiableList() );

        LOGGER.debug( "Here are retries needed: {}", retriesNeeded );

        // With 9 retries and an additional 2 seconds per retry, max sleep will
        // be 90 seconds.
        final int RETRY_LIMIT = 10;
        final Duration RETRY_WAIT_PER_ATTEMPT = Duration.ofSeconds( 2 );

        List<IngestResult> retriesFinished =
                new ArrayList<>( retriesNeeded.size() );

        try
        {
            int retriesAttempted = 0;

            while ( retriesNeeded.size() > 0 && retriesAttempted < RETRY_LIMIT )
            {
                List<IngestResult> doRetryOnThese =
                        new ArrayList<>( retriesNeeded.size() );
                doRetryOnThese.addAll( retriesNeeded );

                LOGGER.debug( "Iteration {}, retries needed: {}",
                              retriesAttempted,retriesNeeded );

                List<CompletableFuture<List<IngestResult>>> retriedIngests =
                        new ArrayList<>( retriesNeeded.size() );
                List<IngestResult> retriesFinishedThisIteration =
                        new ArrayList<>( retriesNeeded.size() );

                // On second and following retries, back off a bit.
                // On the first (0th) attempt, no waiting. After that, a little
                // more time with each iteration.
                Thread.sleep( RETRY_WAIT_PER_ATTEMPT.toMillis() * retriesAttempted );

                for ( IngestResult ingestResult : doRetryOnThese )
                {
                    List<CompletableFuture<List<IngestResult>>> retriedIngest = loader.retry( ingestResult );
                    retriedIngests.addAll( retriedIngest );
                }

                for ( CompletableFuture<List<IngestResult>> futureRetriedIngest : retriedIngests )
                {
                    List<IngestResult> retried = futureRetriedIngest.get();
                    retriesFinishedThisIteration.addAll( retried );
                }

                LOGGER.debug( "Iteration {}, retries finished this iteration: {}",
                              retriesAttempted, retriesFinishedThisIteration );

                retriesAttempted++;
                retriesNeeded = retriesFinishedThisIteration.stream()
                                                            .filter( IngestResult::requiresRetry )
                                                            .collect( Collectors.toUnmodifiableList() );
                retriesFinished.addAll( retriesFinishedThisIteration.stream()
                                                                    .filter( r -> !r.requiresRetry() )
                                                                    .collect( Collectors.toUnmodifiableList() ) );
            }

            LOGGER.debug( "After iterations, all retries finished: {}",
                          retriesFinished );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted during ingest.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            String message = "An ingest task could not be completed.";
            throw new IngestException( message, ee );
        }
        finally
        {
            ingestExecutor.shutdownNow();
        }

        if ( !retriesNeeded.isEmpty() )
        {
            throw new IngestException( "Could not finish ingest because the "
                                       + "following sources required retries "
                                       + "but the retry limit of " + RETRY_LIMIT
                                       + " attempts was reached: "
                                       + retriesNeeded + ". Another WRES "
                                       + "instance may still be ingesting this "
                                       + "data. Please contact the WRES team." );
        }

        // Subtract the yet-to-retry sources, add the completed retried sources.
        List<IngestResult> composedResults = projectSources.stream()
                                                           .filter( r -> !r.requiresRetry() )
                                                           .collect( Collectors.toList() );
        composedResults.addAll( retriesFinished );
        List<IngestResult> safeToShareResults =
                Collections.unmodifiableList( composedResults );

        if ( safeToShareResults.isEmpty() )
        {
            throw new IngestException( "No data were ingested." );
        }

        try
        {
            result = Projects.getProjectFromIngest( systemSettings,
                                                    database,
                                                    executor,
                                                    projectConfig,
                                                    safeToShareResults );

            if ( Operations.shouldAnalyze( safeToShareResults ) )
            {
                database.refreshStatistics( false );
            }
        }
        catch ( SQLException | IngestException | PreIngestException e )
        {
            throw new IngestException( "Failed to finalize ingest.", e );
        }

        return result;
    }

    /**
     * Gracefully shuts down all IO operations
     * @param database The database to use.
     * @param executor The executor to use.
     */
    public static void shutdown( Database database,
                                 Executor executor )
    {
        LOGGER.info("Shutting down the IO layer...");
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
            LOGGER.info("Successfully connected to the database");
        }
        catch (final SQLException e)
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
        database.refreshStatistics(true);
    }

    /**
     * Given a set of ingest results, answer the question "should we analyze?"
     * @param ingestResults the results of ingest
     * @return true if we should run an analyze
     */
    private static boolean shouldAnalyze( List<IngestResult> ingestResults )
    {
        // See if any ill effects still occur when disabling explicit analyze.
        // The schema and process for ingest is quite different now. See #76787.
       // return false;

        
        return wres.util.Collections.exists( ingestResults,
                                             ingestResult -> !ingestResult.wasFoundAlready() );
        
    }

    public static void createNetCDFOutputTemplate(final String sourceName, final String templateName)
            throws IOException
    {
        try (NetCDFCopier
                writer = new NetCDFCopier( sourceName, templateName, ZonedDateTime.now(  ) ))
        {
            writer.write();
        }
    }
}
