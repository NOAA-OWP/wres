package wres.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Projects;
import wres.io.data.caching.UnitConversions;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.project.Project;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.SourceLoader;
import wres.io.retrieval.DataGenerator;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.writing.netcdf.NetCDFCopier;
import wres.system.ProgressMonitor;
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
        Future unitConversionLoad = Executor.execute( UnitConversions::initialize );

        final String INTERRUPTED_VARIABLE_VALIDATION_MESSAGE =
                "The process for determining if '{}' is a valid variable was interrupted.";

        boolean isVector;
        Future<Boolean> leftValid = null;
        Future<Boolean> rightValid = null;
        Future<Boolean> baselineValid = null;
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

        if (isVector)
        {
            leftValid = Executor.submit(
                    () -> Variables.isObservationValid( project.getId(),
                                                        Project.LEFT_MEMBER,
                                                        project.getLeftVariableID()
                    ) );
        }

        if (isVector && ConfigHelper.isForecast( project.getRight() ))
        {
            rightValid = Executor.submit(
                    () -> Variables.isForecastValid( project.getId(),
                                                     Project.RIGHT_MEMBER,
                                                     project.getRightVariableID()
                    )
            );
        }
        else if (isVector)
        {
            rightValid = Executor.submit(
                    () -> Variables.isObservationValid( project.getId(),
                                                        Project.RIGHT_MEMBER,
                                                        project.getRightVariableID()
                    )
            );
        }

        if ( isVector && project.getBaseline() != null && ConfigHelper.isForecast( project.getBaseline() ))
        {
            baselineValid = Executor.submit(
                    () -> Variables.isForecastValid( project.getId(),
                                                     Project.BASELINE_MEMBER,
                                                     project.getBaselineVariableID()
                    )
            );
        }
        else if ( isVector && project.getBaseline() != null)
        {
            baselineValid = Executor.submit(
                    () -> Variables.isObservationValid( project.getId(),
                                                        Project.BASELINE_MEMBER,
                                                        project.getBaselineVariableID()
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

        try
        {
            unitConversionLoad.get();
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "The process for pre-loading unit conversions was interrupted.",
                         e );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            // If loading failed, it will attempt to try again later.
            LOGGER.warn( "The process for pre-loading unit conversions failed.",
                         e );
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
            Boolean leftIsValid = leftValid.get();

            if (!leftIsValid)
            {

                List<String> availableVariables = Variables.getAvailableObservationVariables(
                        project.getId(),
                        Project.LEFT_MEMBER
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
            Boolean rightIsValid = rightValid.get();

            if (!rightIsValid)
            {
                List<String> availableVariables;

                if (ConfigHelper.isForecast( project.getRight() ))
                {
                    availableVariables = Variables.getAvailableForecastVariables(
                            project.getId(),
                            Project.RIGHT_MEMBER
                    );
                }
                else
                {
                    availableVariables = Variables.getAvailableObservationVariables(
                            project.getId(),
                            Project.RIGHT_MEMBER
                    );
                }

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
        if (baselineValid == null)
        {
            LOGGER.info("Preliminary metadata loading is complete.");
            return;
        }

        try
        {
            Boolean baselineIsValid = baselineValid.get();

            if (!baselineIsValid)
            {
                List<String> availableVariables;

                if (ConfigHelper.isForecast( project.getBaseline() ))
                {
                    availableVariables = Variables.getAvailableForecastVariables(
                            project.getId(),
                            Project.BASELINE_MEMBER
                    );
                }
                else
                {
                    availableVariables = Variables.getAvailableObservationVariables(
                            project.getId(),
                            Project.BASELINE_MEMBER
                    );
                }

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
     * @param projectConfig the projectConfig for the evaluation
     * @return the {@link Project} (state about this evaluation)
     * @throws IOException when anything goes wrong
     * @throws IllegalStateException when another process already holds lock.
     */
    public static Project ingest( ProjectConfig projectConfig )
            throws IOException
    {
        Database.lockForMutation();

        try
        {
            return Operations.doIngestWork( projectConfig );
        }
        finally
        {
            Database.releaseLockForMutation();
        }
    }

    /**
     * Ingests and returns the hashes of source files involved in this project.
     * TODO: Find a more appropriate location; this should call the ingest logic, not implement it
     * @param projectConfig the projectConfig to ingest
     * @return the projectdetails object from ingesting this project
     * @throws IOException when anything goes wrong
     */
    private static Project doIngestWork( ProjectConfig projectConfig )
            throws IOException
    {
        boolean orphansDeleted;
        try
        {
            orphansDeleted = Database.removeOrphanedData();
        }
        catch ( SQLException e )
        {
            throw new IOException( "Ingest failed when attempting to remove orphaned data.", e );
        }

        // If data had been removed, it needs to be officially vacuumed up and
        // statistics need to be reevaluated.
        if (orphansDeleted)
        {
            LOGGER.info("Since previously incomplete data was removed, the "
                        + "state of the database will now be refreshed.");
            try
            {
                Database.refreshStatistics( true );
            }
            catch ( SQLException se )
            {
                throw new IOException( "Failed to refresh statistics.", se );
            }
        }

        Project result = null;

        List<IngestResult> projectSources = new ArrayList<>();

        SourceLoader loader = new SourceLoader( projectConfig);
        try
        {
            List<Future<List<IngestResult>>> ingestions = loader.load();
            ProgressMonitor.setSteps( (long)ingestions.size());

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( ingestions.size() + " direct ingest results." );
            }

            for (Future<List<IngestResult>> task : ingestions)
            {
                List<IngestResult> ingested = task.get();
                ProgressMonitor.completeStep();
                projectSources.addAll( ingested );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted during ingest.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            String message = "An ingest task could not be completed.";
            throw new IngestException( message, e );
        }
        finally
        {
            List<IngestResult> leftovers = Database.completeAllIngestTasks();

            IngestedValues.complete();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( leftovers.size() + " indirect ingest results" );
            }
            projectSources.addAll( leftovers );
        }

        LOGGER.debug( "Here are the files ingested: {}", projectSources );

        List<IngestResult> safeToShareResults =
                Collections.unmodifiableList( projectSources );

        try
        {
            result = Projects.getProjectFromIngest( projectConfig,
                                                    safeToShareResults );

            if ( Operations.shouldAnalyze( safeToShareResults ) )
            {
                Database.addNewIndexes();
                Database.refreshStatistics( false );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while finalizing ingest.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( SQLException | ExecutionException e )
        {
            throw new IngestException( "Failed to finalize ingest.", e );
        }

        if ( result == null )
        {
            throw new IngestException( "Result of ingest was null" );
        }

        return result;
    }


    // TODO: Why are we passing a feature and not a FeatureDetails object, which is custom made for all of this?
    public static DataGenerator getInputs( Project project,
                                           Feature feature,
                                           Path outputDirectoryForPairs )
    {
        return new DataGenerator( feature,
                                  project,
                                  outputDirectoryForPairs );
    }

    /**
     * Gracefully shuts down all IO operations
     */
    public static void shutdown()
    {
        LOGGER.info("Shutting down the IO layer...");

        try
        {
            Database.addNewIndexes();
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while adding indices during shutdown.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( SQLException | ExecutionException e )
        {
            LOGGER.warn( "Failed to add indices while shutting down.", e );
        }

        Executor.complete();
        Database.shutdown();
    }

    /**
     * Forces the IO to shutdown all IO operations without finishing stored tasks
     * @param timeOut The amount of time to wait while shutting down tasks
     * @param timeUnit The unit with which to measure the shutdown
     */
    public static void forceShutdown( long timeOut, TimeUnit timeUnit )
    {
        LOGGER.info( "Forcefully shutting down the IO module..." );

        try
        {
            Database.addNewIndexes();
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while adding indices during shutdown.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( SQLException | ExecutionException e )
        {
            LOGGER.warn( "Failed to add indices while shutting down.", e );
        }

        List<Runnable> executorTasks =
                Executor.forceShutdown( timeOut / 2, timeUnit );
        List<Runnable> databaseTasks =
                Database.forceShutdown( timeOut / 2, timeUnit );

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
     * @throws SQLException when WRES cannot access the database
     */
    public static void testConnection() throws SQLException
    {
        try
        {
            final DataScripter script = new DataScripter( "SELECT version() as version_detail" );
            final String version = script.retrieve( "version_detail" );
            LOGGER.info(version);
            LOGGER.info("Successfully connected to the database");
        }
        catch (final SQLException e)
        {
            throw new SQLException( "Could not connect to the database.", e );
        }
    }

    /**
     * Removes all loaded user information from the database
     * @throws IOException when locking for changes fails
     * @throws SQLException when cleaning or refreshing stats fails
     */
    public static void cleanDatabase() throws IOException, SQLException
    {
        Database.lockForMutation();
        Database.clean();
        Database.refreshStatistics( true );
        Database.releaseLockForMutation();
    }

    /**
     * Updates the statistics and removes all dead rows from the database
     * @throws SQLException if the orphaned data could not be removed or the refreshing of statistics fails
     */
    public static void refreshDatabase() throws SQLException
    {
        try
        {
            Database.lockForMutation();
        }
        catch ( IOException e )
        {
            throw new SQLException( "Database mutation could not be locked for statistical refresh.", e );
        }

        Database.removeOrphanedData();
        Database.refreshStatistics(true);

        try
        {
            Database.releaseLockForMutation();
        }
        catch ( IOException e )
        {
            throw new SQLException( "The lock for database mutation could not be properly released.", e );
        }
    }

    /**
     * Logs information about the execution of the WRES into the database for
     * aid in remote debugging
     * @param arguments The arguments used to run the WRES
     * @param start The time (in milliseconds) at which the WRES was executed
     * @param duration The length of time (in milliseconds) that the WRES
     *                 executed in
     * @param failed Whether or not the execution failed
     * @param error Any error that caused the WRES to crash
     * @param version The top-level version of WRES (module versions vary)
     */
    public static void logExecution( String[] arguments,
                                     long start,
                                     long duration,
                                     boolean failed,
                                     String error,
                                     String version )
    {
        Objects.requireNonNull( arguments );
        Objects.requireNonNull( version );

        try
        {
            Timestamp startTimestamp = new Timestamp( start );
            String runTime = duration + " MILLISECONDS";


            // For any arguments that happen to be regular files, read the
            // contents of the first file into the "project" field. Maybe there
            // is an improvement that can be made, but this should cover the
            // common case of a single file in the args.
            String project = "";
            List<String> commandsAcceptingFiles = Arrays.asList( "execute",
                                                                 "ingest" );

            if ( commandsAcceptingFiles.contains( arguments[0].toLowerCase() ) )
            {
                for ( String arg : arguments )
                {
                    Path path = Paths.get( arg );

                    if ( path.toFile()
                             .isFile() )
                    {
                        project = String.join( System.lineSeparator(),
                                               Files.readAllLines( path ) );

                        // Since this is an xml column, only go for first file.
                        break;
                    }
                }
            }

            DataScripter script = new DataScripter(  );

            script.addLine("INSERT INTO wres.ExecutionLog (");
            script.addTab().addLine("arguments,");
            script.addTab().addLine("system_version,");
            script.addTab().addLine("project,");
            script.addTab().addLine("username,");
            script.addTab().addLine("address,");
            script.addTab().addLine("start_time,");
            script.addTab().addLine("run_time,");
            script.addTab().addLine("failed,");
            script.addTab().addLine("error");
            script.addLine(")");
            script.addLine("VALUES (");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("inet_client_addr(),");
            script.addTab().addLine("?,");
            script.addTab().addLine("CAST(? AS INTERVAL),");
            script.addTab().addLine("?,");
            script.addTab().addLine("?");
            script.addLine(");");

            script.execute( String.join(" ", arguments),
                            version,
                          project,
                          System.getProperty( "user.name" ),
                          // Let server find and report username
                          // Let server find and report network address
                          startTimestamp,
                          runTime,
                          failed,
                          error );
        }
        catch ( SQLException | IOException e )
        {
            LOGGER.warn( "Execution metadata could not be logged to the database.",
                         e );
        }
    }

    /**
     * Creates a set of {@link wres.config.FeaturePlus Features} to
     * evaluate statistics for
     * @param project The object that holds details on what a project
     *                       should do
     * @return A set of {@link wres.config.FeaturePlus Features}
     * @throws SQLException Thrown if information about the features could not
     * be retrieved from the database
     * @throws IOException Thrown if IO operations prevented the set from being
     * created
     * @throws NoDataException if there are no features to process
     */
    public static Set<FeaturePlus> decomposeFeatures( Project project )
            throws SQLException, IOException
    {
        Set<FeaturePlus> atomicFeatures = new TreeSet<>( Comparator.comparing(
                ConfigHelper::getFeatureDescription ));

        for (FeatureDetails details : project.getFeatures())
        {
            // Check if the feature has any intersecting values
            Feature feature = details.toFeature();

            boolean hasLeadOffset;

            try
            {
                hasLeadOffset = project.getLeadOffset( feature ) != null;
            }
            catch ( CalculationException e )
            {
                throw new IOException( "It could not be determined whether or "
                                       + "not left and right hand data intersect at " +
                                       ConfigHelper.getFeatureDescription( feature ) );
            }

            if ( project.usesGriddedData( project.getRight() ) ||
                 hasLeadOffset)
            {
                Feature resolvedFeature = details.toFeature();
                atomicFeatures.add( FeaturePlus.of ( resolvedFeature ));
            }
            else
            {
                LOGGER.info( "The location '{}' will not be evaluated because "
                             + "it doesn't have any intersecting data between "
                             + "left and right inputs.", details );
            }
        }

        // Nothing to process, which is exceptional behavior
        if( atomicFeatures.isEmpty() )
        {
            throw new NoDataException( "There were no features to process." );
        }

        return Collections.unmodifiableSet( atomicFeatures );
    }


    /**
     * Given a set of ingest results, answer the question "should we analyze?"
     * @param ingestResults the results of ingest
     * @return true if we should run an analyze
     */
    private static boolean shouldAnalyze( List<IngestResult> ingestResults )
    {
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
