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
import wres.grid.client.Fetcher;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Projects;
import wres.io.data.caching.UnitConversions;
import wres.io.data.caching.Variables;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.ProjectDetails;
import wres.io.griddedReader.GriddedReader;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.SourceLoader;
import wres.io.reading.TimeSeriesValues;
import wres.io.reading.fews.PIXMLReader;
import wres.io.retrieval.InputGenerator;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.io.utilities.ScriptBuilder;
import wres.io.writing.PairWriter;
import wres.io.writing.netcdf.NetCDFCopier;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.util.ProgressMonitor;

public final class Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Operations.class);

    private Operations ()
    {
    }

    /**
     * Prepares IO operations for evaluation execution checks if evaluations
     * are valid/possible
     * @param projectDetails The project to evaluate
     * @throws IOException Thrown if the project itself could not be prepared
     * @throws IOException Thrown if the process for determining whether or not
     * variables are valid fails
     * @throws NoDataException Thrown if a variable to evaluate is not
     * accessible to the evaluation
     */
    public static void prepareForExecution( ProjectDetails projectDetails ) throws IOException
    {
        LOGGER.info("Loading preliminary metadata...");
        Future unitConversionLoad = Executor.execute( UnitConversions::initialize );

        boolean isVector = true;
        Future<Boolean> leftValid = null;
        Future<Boolean> rightValid = null;
        Future<Boolean> baselineValid = null;
        try
        {
            isVector = !(projectDetails.usesGriddedData( projectDetails.getLeft() ) ||
                        projectDetails.usesGriddedData( projectDetails.getRight() ));
        }
        catch ( SQLException e )
        {
            throw new IOException("Could not determine if this project uses "
                                  + "gridded data or not.", e);
        }

        if (isVector)
        {
            leftValid = Executor.submit(
                    () -> Variables.isObservationValid( projectDetails.getId(),
                                                        ProjectDetails.LEFT_MEMBER,
                                                        projectDetails.getLeftVariableID()
                    ) );
        }

        if (isVector && ConfigHelper.isForecast( projectDetails.getRight() ))
        {
            rightValid = Executor.submit(
                    () -> Variables.isForecastValid( projectDetails.getId(),
                                                     ProjectDetails.RIGHT_MEMBER,
                                                     projectDetails.getRightVariableID()
                    )
            );
        }
        else if (isVector)
        {
            rightValid = Executor.submit(
                    () -> Variables.isObservationValid( projectDetails.getId(),
                                                        ProjectDetails.RIGHT_MEMBER,
                                                        projectDetails.getRightVariableID()
                    )
            );
        }

        if (isVector && projectDetails.getBaseline() != null && ConfigHelper.isForecast( projectDetails.getBaseline() ))
        {
            baselineValid = Executor.submit(
                    () -> Variables.isForecastValid( projectDetails.getId(),
                                                     ProjectDetails.BASELINE_MEMBER,
                                                     projectDetails.getBaselineVariableID()
                    )
            );
        }
        else if (isVector && projectDetails.getBaseline() != null)
        {
            baselineValid = Executor.submit(
                    () -> Variables.isObservationValid( projectDetails.getId(),
                                                        ProjectDetails.BASELINE_MEMBER,
                                                        projectDetails.getBaselineVariableID()
                    )
            );
        }

        try
        {
            projectDetails.prepareForExecution();
        }
        catch (SQLException exception)
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
            LOGGER.error("The process for loading unit conversions was interrupted.");
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            // If loading failed, it will attempt to try again later.
            LOGGER.error("The process for loading unit conversions failed.", e);
        }

        // If we're performing gridded evaluation, we can't check if our
        // variables are valid via normal means, so just return
        if (!isVector)
        {
            return;
        }

        // TODO: Split logic out into separate functions
        try
        {
            Boolean leftIsValid = leftValid.get();

            if (!leftIsValid)
            {

                List<String> availableVariables = Variables.getAvailableObservationVariables(
                        projectDetails.getId(),
                        ProjectDetails.LEFT_MEMBER
                );

                String message = "There is no '"
                                 + projectDetails.getLeft().getVariable().getValue()
                                 + "' data available for the left hand data "
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
            LOGGER.error("The process for determining if '"
                         + projectDetails.getLeft().getVariable().getValue()
                         + "' is a valid variable was interrupted.");
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            throw new IOException( "An error occurred while determining whether '"
                                   + projectDetails.getLeft().getVariable().getValue()
                                   + "' is a valid variable for left side evaluation.",
                                   e );
        }
        catch ( SQLException e )
        {
            throw new IOException("'"
                                  + projectDetails.getLeft().getVariable().getValue()
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

                if (ConfigHelper.isForecast( projectDetails.getRight() ))
                {
                    availableVariables = Variables.getAvailableForecastVariables(
                            projectDetails.getId(),
                            ProjectDetails.RIGHT_MEMBER
                    );
                }
                else
                {
                    availableVariables = Variables.getAvailableObservationVariables(
                            projectDetails.getId(),
                            ProjectDetails.RIGHT_MEMBER
                    );
                }

                String message = "There is no '"
                                 + projectDetails.getRightVariableName()
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
            LOGGER.error("The process for determining if '"
                         + projectDetails.getRightVariableName()
                         + "' is a valid variable was interrupted.");
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            throw new IOException( "An error occurred while determining whether '"
                                   + projectDetails.getRightVariableName()
                                   + "' is a valid variable for right side evaluation.",
                                   e );
        }
        catch ( SQLException e )
        {
            throw new IOException("'"
                                  + projectDetails.getRightVariableName()
                                  + "' is not a valid variable for right hand "
                                  + "evaluation. Possible alternatives could "
                                  + "not be found.", e);
        }

        // If baselineValid is null, then we have no baseline variable to
        // evaluate; it is safe to exit.
        if (baselineValid == null)
        {
            return;
        }

        try
        {
            Boolean baselineIsValid = baselineValid.get();

            if (!baselineIsValid)
            {
                List<String> availableVariables;

                if (ConfigHelper.isForecast( projectDetails.getRight() ))
                {
                    availableVariables = Variables.getAvailableForecastVariables(
                            projectDetails.getId(),
                            ProjectDetails.BASELINE_MEMBER
                    );
                }
                else
                {
                    availableVariables = Variables.getAvailableObservationVariables(
                            projectDetails.getId(),
                            ProjectDetails.BASELINE_MEMBER
                    );
                }

                String message = "There is no '"
                                 + projectDetails.getBaseline().getVariable().getValue()
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
            LOGGER.error("The process for determining if '"
                         + projectDetails.getBaseline().getVariable().getValue()
                         + "' is a valid variable was interrupted.");
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            throw new IOException( "An error occurred while determining whether '"
                                   + projectDetails.getBaseline().getVariable().getValue()
                                   + "' is a valid variable for baseline evaluation.",
                                   e );
        }
        catch ( SQLException e )
        {
            throw new IOException("'"
                                  + projectDetails.getBaseline().getVariable().getValue()
                                  + "' is not a valid variable for right hand "
                                  + "evaluation. Possible alternatives could "
                                  + "not be found.", e);
        }
    }

    /**
     * Ingests and returns the hashes of source files involved in this project.
     * @param projectConfig the projectConfig to ingest
     * @return the projectdetails object from ingesting this project
     * @throws IOException when anything goes wrong
     */
    public static ProjectDetails ingest( ProjectConfig projectConfig )
            throws IOException
    {
        // First, ensure this process is the only one ingesting
        Database.lockForMutation();

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

        ProjectDetails result = null;

        List<IngestResult> projectSources = new ArrayList<>();

        SourceLoader loader = new SourceLoader(projectConfig);
        try {
            List<Future<List<IngestResult>>> ingestions = loader.load();
            ProgressMonitor.setSteps((long)ingestions.size());

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
            PIXMLReader.saveLeftoverForecasts();
            TimeSeriesValues.complete();
        }
        catch ( InterruptedException ie )
        {
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException e )
        {
            String message = "An ingest task could not be completed.";
            throw new IngestException( message, e );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to save leftover forecasts", se );
        }
        finally
        {
            List<IngestResult> leftovers = Database.completeAllIngestTasks();
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
            LOGGER.warn( "Interrupted while finalizing ingest." );
            Thread.currentThread().interrupt();
        }
        catch ( SQLException | ExecutionException e )
        {
            throw new IngestException( "Failed to finalize ingest.", e );
        }

        Database.releaseLockForMutation();

        if ( result == null )
        {
            throw new IngestException( "Result of ingest was null" );
        }

        return result;
    }


    public static InputGenerator getInputs( ProjectDetails projectDetails,
                                            Feature feature )
    {
        return new InputGenerator( feature, projectDetails );
    }

    /**
     * Builds the database up to current specifications
     */
    public static void install()
    {
        Database.buildInstance();
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
            Thread.currentThread().interrupt();
        }
        catch ( SQLException | ExecutionException e )
        {
            LOGGER.warn( "Failed to add indices while shutting down.", e );
        }

        Executor.complete();
        NetcdfOutputWriter.close();
        Database.shutdown();
        PairWriter.flushAndCloseAllWriters();
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
        PairWriter.flushAndCloseAllWriters();
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
            final String version = Database.getResult("Select version() "
                                                      + "AS version_detail",
                                                      "version_detail");
            LOGGER.info(version);
            LOGGER.info("Successfully connected to the database");
        }
        catch (final SQLException e) {
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
        Database.removeOrphanedData();
        Database.refreshStatistics(true);
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

            ScriptBuilder script = new ScriptBuilder(  );

            script.addLine("INSERT INTO ExecutionLog (");
            script.addTab().addLine("arguments,");
            script.addTab().addLine("system_settings,");
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
            script.addTab().addLine("current_user,");
            script.addTab().addLine("inet_client_addr(),");
            script.addTab().addLine("?,");
            script.addTab().addLine("CAST(? AS INTERVAL),");
            script.addTab().addLine("?,");
            script.addTab().addLine("?");
            script.addLine(");");

            script.execute( String.join(" ", arguments),
                            version,
                          project,
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
     * @param projectDetails The object that holds details on what a project
     *                       should do
     * @return A set of {@link wres.config.FeaturePlus Features}
     * @throws SQLException Thrown if information about the features could not
     * be retrieved from the database
     * @throws IOException Thrown if IO operations prevented the set from being
     * created
     */
    public static Set<FeaturePlus> decomposeFeatures( ProjectDetails projectDetails )

            throws SQLException, IOException
    {
        Set<FeaturePlus> atomicFeatures = new TreeSet<>( Comparator.comparing(
                ConfigHelper::getFeatureDescription ));

        for (FeatureDetails details : projectDetails.getFeatures())
        {
            // Check if the feature has any intersecting values
            Feature feature = details.toFeature();

            if ( projectDetails.usesGriddedData( projectDetails.getRight() ) ||
                 projectDetails.getLeadOffset( feature ) != null)
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


    /**
     * Get the count of leads for a project
     * TODO: optimize
     * @param projectIdentifier the code/hash of a project (not row id)
     * @return the count of distinct leads found across all forecasts in project
     * @throws SQLException if the count could not be determined
     */

    public static long getLeadCountsForProject( String projectIdentifier )
            throws SQLException
    {
        final String NEWLINE = System.lineSeparator();
        final String LABEL = "total_leads";
        String query = "SELECT count( distinct( FV.lead ) ) AS "
                       + LABEL + NEWLINE
                       + "FROM wres.TimeSeries TS" + NEWLINE
                       + "INNER JOIN wres.ForecastValue FV" + NEWLINE
                       + "    ON TS.timeseries_id = FV.timeseries_id" + NEWLINE
                       + "WHERE EXISTS (" + NEWLINE
                       + "        SELECT 1" + NEWLINE
                       + "        FROM wres.ProjectSource PS" + NEWLINE
                       + "        INNER JOIN wres.ForecastSource FS" + NEWLINE
                       + "            ON FS.source_id = PS.source_id" + NEWLINE
                       + "        INNER JOIN wres.Project P" + NEWLINE
                       + "            ON P.project_id = PS.project_id" + NEWLINE
                       + "        WHERE P.input_code = "
                       + projectIdentifier + NEWLINE
                       + "            AND FS.forecast_id = TS.timeseries_id" + NEWLINE
                       + "    );";
        return (long) Database.getResult( query, LABEL );
    }


    /**
     * Get the count of basis times for a project
     * TODO: optimize
     * @param projectIdentifier the code/hash of a project (not row id)
     * @return the count of distinct basis times found across all forecasts
     * in the project
     * @throws SQLException if the count could not be determined
     */
    public static long getBasisTimeCountsForProject( String projectIdentifier )
            throws SQLException
    {
        final String NEWLINE = System.lineSeparator();
        final String LABEL = "total_bases";
        String query = "SELECT count( distinct( TS.initialization_date ) ) AS "
                       + LABEL + NEWLINE
                       + "FROM wres.TimeSeries TS" + NEWLINE
                       + "WHERE EXISTS (" + NEWLINE
                       + "        SELECT 1" + NEWLINE
                       + "        FROM wres.ProjectSource PS" + NEWLINE
                       + "        INNER JOIN wres.ForecastSource FS" + NEWLINE
                       + "            ON FS.source_id = PS.source_id" + NEWLINE
                       + "        INNER JOIN wres.Project P" + NEWLINE
                       + "            ON P.project_id = PS.project_id"
                       + "        WHERE P.input_code = "
                       + projectIdentifier + NEWLINE
                       + "            AND FS.forecast_id = TS.timeseries_id" + NEWLINE
                       + "    );";
        return (long) Database.getResult( query, LABEL );
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
