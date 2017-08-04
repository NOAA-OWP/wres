package wres.io;

import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.MetricInput;
import wres.io.concurrency.Executor;
import wres.io.concurrency.PairRetriever;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.grouping.LabeledScript;
import wres.io.reading.SourceLoader;
import wres.io.reading.fews.PIXMLReader;
import wres.io.utilities.Database;
import wres.io.utilities.InputGenerator;
import wres.io.utilities.ScriptGenerator;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
    public static Map<Integer, Future<MetricInput>> getPairs (ProjectConfig projectConfig,
                                                                         Conditions.Feature feature) throws SQLException
    {
        Integer variableId = ConfigHelper.getVariableID(projectConfig.getInputs().getRight());

        LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(variableId);

        Integer finalLead = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());
        Map<Integer, Future<MetricInput>> threadResults = new TreeMap<>();

        int step = 1;

        while (ConfigHelper.leadIsValid(projectConfig, step, finalLead))
        {
            threadResults.put(step, getPairs(projectConfig, feature, step));
            step++;
        }

        return threadResults;
    }

    public static InputGenerator getInputs(ProjectConfig projectConfig, Conditions.Feature feature)
    {
        return new InputGenerator(projectConfig, feature);
    }

    public static Future<MetricInput> getPairs(ProjectConfig projectConfig, Conditions.Feature feature, int windowNumber)
    {
        PairRetriever pairRetriever = new PairRetriever(projectConfig, feature, windowNumber);
        pairRetriever.setOnRun(ProgressMonitor.onThreadStartHandler());
        pairRetriever.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
        return Database.submit(pairRetriever);
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

    public static void logExecution(String arguments, String project, String start, String stop, boolean failed)
    {
        try {
            String systemConfiguration = SystemSettings.getRawConfiguration();
            String username = SystemSettings.getUserName();

            String address;

            try
            {
                address = String.valueOf(InetAddress.getLocalHost());
            }
            catch (UnknownHostException e) {
                LOGGER.error(Strings.getStackTrace(e));
                address = "Unknown";
            }

            if (project == null || project.isEmpty())
            {
                project = "null";
            }
            else
            {
                project = "'" + project + "'::xml";
            }


            StringBuilder script = new StringBuilder();

            script.append("INSERT INTO ExecutionLog(")
                  .append("arguments, ")
                  .append("system_settings, ")
                  .append("project, ")
                  .append("username, ")
                  .append("address, ")
                  .append("start_time, ")
                  .append("run_time, ")
                  .append("failed) ");
            script.append("VALUES (")
                  .append("'").append(arguments).append("', ")
                  .append("'").append(systemConfiguration).append("', ")
                  .append(project).append(", ")
                  .append("'").append(username).append("', ")
                  .append("'").append(address).append("', ")
                  .append("'").append(start).append("'::timestamp, ")
                  .append("'").append(stop).append("'::timestamp - '").append(start).append("'::timestamp")
                  .append(", ").append(String.valueOf(failed)).append(");");

            Database.execute(script.toString());
        }
        catch (FileNotFoundException | XMLStreamException | TransformerException e)
        {
            LOGGER.error("The system configuration could not be loaded. Execution information was not logged to the database.");
            LOGGER.error(Strings.getStackTrace(e));
        }
        catch (SQLException e) {
            LOGGER.error("Execution information could not be saved to the database.");
            LOGGER.error(Strings.getStackTrace(e));
        }
    }

}
