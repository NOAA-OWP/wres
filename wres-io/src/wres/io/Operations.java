package wres.io;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;

import org.slf4j.LoggerFactory;

import wres.config.generated.Conditions;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.config.SystemSettings;
import wres.io.reading.SourceLoader;
import wres.io.reading.fews.PIXMLReader;
import wres.io.utilities.Database;
import wres.io.utilities.InputGenerator;
import wres.util.Strings;

public final class Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Operations.class);

    private static final boolean SUCCESS = true;
    private static final boolean FAILURE = false;

    private Operations ()
    {
    }

    public static boolean ingest(ProjectConfig projectConfig)
    {
        boolean completedSmoothly;

        SourceLoader loader = new SourceLoader(projectConfig);
        try {
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
        finally
        {
            Database.restoreAllIndices();
        }

        return completedSmoothly;
    }

    public static InputGenerator getInputs(ProjectConfig projectConfig, Conditions.Feature feature)
    {
        return new InputGenerator(projectConfig, feature);
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
        Database.refreshStatistics(true);
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


            String script = "INSERT INTO ExecutionLog(" +
                            "arguments, " +
                            "system_settings, " +
                            "project, " +
                            "username, " +
                            "address, " +
                            "start_time, " +
                            "run_time, " +
                            "failed) " +
                            "VALUES (" +
                            "'" + arguments + "', " +
                            "'" + systemConfiguration + "', " +
                            project + ", " +
                            "'" + username + "', " +
                            "'" + address + "', " +
                            "'" + start + "'::timestamp, " +
                            "'" + stop + "'::timestamp - '" + start
                            + "'::timestamp" +
                            ", " + String.valueOf( failed ) + ");";

            Database.execute( script );
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
