package wres.io;

import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.io.concurrency.Executor;
import wres.io.concurrency.PairRetriever;
import wres.io.config.ConfigHelper;
import wres.io.grouping.LabeledScript;
import wres.io.reading.SourceLoader;
import wres.io.reading.fews.PIXMLReader;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptGenerator;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public final class Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Operations.class);

    private static final boolean SUCCESS = true;
    private static final boolean FAILURE = false;
    private static Operations ourInstance = new Operations();

    public static Operations getInstance () {
        return ourInstance;
    }

    private Operations ()
    {
    }

    public static boolean ingest(ProjectConfig projectConfig)
    {
        boolean completedSmoothly = FAILURE;

        SourceLoader loader = new SourceLoader(projectConfig);
        try {
            Database.suspendAllIndices();

            List<Future> ingestions = loader.load();

            for (Future task : ingestions)
            {
                try
                {
                    if (!task.isDone())
                    {
                        task.get();
                    }
                }
                catch (InterruptedException | ExecutionException e)
                {
                    LOGGER.error("");
                    LOGGER.error("An asynchonrous ingest task could not be completed.");
                    LOGGER.error("");
                    LOGGER.error(Strings.getStackTrace(e));
                    LOGGER.error("");
                }
            }

            PIXMLReader.saveLeftoverForecasts();
            Database.completeAllIngestTasks();

            completedSmoothly = SUCCESS;
        }
        catch (IOException e)
        {
            LOGGER.error("");
            LOGGER.error(Strings.getStackTrace(e));
            LOGGER.error("");
        }
        finally
        {
            Database.restoreAllIndices();
            Database.refreshStatistics();
        }

        return completedSmoothly;
    }

    /**
     * Creates a mapping between a window number and the pairs that belong to it for a given location
     *
     * <p>
     *     A description for a window number may be determined by calling
     *     {@link wres.io.config.ConfigHelper#getLeadQualifier(ProjectConfig, int)
     *     ConfigHelper.getLeadQualifier(projectConfig, windowNumber)}
     * </p>
     * @param projectConfig The project configuration that determines what sort of pairs to bring back
     * @param feature The configured location of the data that needs to be retrieved
     * @return A mapping between the number of a window and and its pairs
     * @throws SQLException Thrown if there was an error when communicating with the database
     */
    public static Map<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>> getPairs (ProjectConfig projectConfig,
                                                                         Conditions.Feature feature) throws SQLException
    {
        Integer variableId = ConfigHelper.getVariableID(projectConfig.getInputs().getRight());

        LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(variableId);

        Integer finalLead = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());
        Map<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>> threadResults = new TreeMap<>();

        int step = 1;

        while (ConfigHelper.leadIsValid(projectConfig, step, finalLead))
        {
            PairRetriever pairRetriever = new PairRetriever(projectConfig, feature, step);
            pairRetriever.setOnRun(ProgressMonitor.onThreadStartHandler());
            pairRetriever.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            threadResults.put(step, Database.submit(pairRetriever));
            step++;
        }

        return threadResults;
    }

    public static void install()
    {
        Database.buildInstance();
    }

    public static void shutdown()
    {
        LOGGER.info("Shutting down the IO layer...");
        Database.restoreAllIndices();
        Executor.complete();
        Database.shutdown();
    }

    public static boolean testConnection()
    {
        boolean result = FAILURE;
        try {
            final String version = Database.getResult("Select version() AS version_detail", "version_detail");
            LOGGER.info(version);
            LOGGER.info("Successfully connected to the database");
            result = SUCCESS;
        }
        catch (final SQLException e) {
            LOGGER.error("Could not connect to database because:");
            LOGGER.error(Strings.getStackTrace(e));
        }
        catch (final RuntimeException exception)
        {
            LOGGER.error(Strings.getStackTrace(exception));
        }
        return result;
    }

    public static boolean cleanDatabase()
    {
        boolean successfullyCleaned = FAILURE;
        try {
            Database.clean();
            successfullyCleaned = SUCCESS;
        }
        catch (SQLException e) {
            LOGGER.error(Strings.getStackTrace(e));
        }
        return  successfullyCleaned;
    }

    public static boolean refreshDatabase()
    {
        Database.refreshStatistics();
        return SUCCESS;
    }

}
