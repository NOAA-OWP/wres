package wres.io;

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerException;

import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.Projects;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.SourceLoader;
import wres.io.reading.fews.PIXMLReader;
import wres.io.retrieval.InputGenerator;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.io.writing.PairWriter;
import wres.util.Strings;

public final class Operations {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Operations.class);

    private static final boolean SUCCESS = true;
    private static final boolean FAILURE = false;

    private Operations ()
    {
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
        ProjectDetails result;

        List<IngestResult> projectSources = new ArrayList<>();

        SourceLoader loader = new SourceLoader(projectConfig);
        try {
            List<Future<List<IngestResult>>> ingestions = loader.load();

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( ingestions.size() + " direct ingest results." );
            }

            for (Future<List<IngestResult>> task : ingestions)
            {
                List<IngestResult> ingested = task.get();
                projectSources.addAll( ingested );
            }
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
        finally
        {
            PIXMLReader.saveLeftoverForecasts();
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
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to finalize ingest.", se );
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
        Database.addNewIndexes();
        Executor.complete();
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
        Database.addNewIndexes();
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
     * @return Whether or not the WRES can access the database
     */
    public static boolean testConnection()
    {
        boolean result = FAILURE;
        try
        {
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

    /**
     * Removes all loaded user information from the database
     * @return Whether or not the operation was a success
     */
    public static boolean cleanDatabase()
    {
        boolean successfullyCleaned = FAILURE;
        try
        {
            Database.clean();
            Database.refreshStatistics( true );
            successfullyCleaned = SUCCESS;
        }
        catch (SQLException e)
        {
            LOGGER.error(Strings.getStackTrace(e));
        }
        return  successfullyCleaned;
    }

    /**
     * Updates the statistics and removes all dead rows from the database
     * @return Whether or not the operation was a success
     */
    public static boolean refreshDatabase()
    {
        Database.refreshStatistics(true);
        return SUCCESS;
    }

    /**
     * Logs information about the execution of the WRES into the database for
     * aid in remote debugging
     * @param arguments The arguments used to run the WRES
     * @param start The time (in milliseconds) at which the WRES was executed
     * @param duration The length of time (in milliseconds) that the WRES
     *                 executed in
     * @param error Any error that caused the WRES to crash
     */
    public static void logExecution( String[] arguments,
                                     long start,
                                     long duration,
                                     String error)
    {
        String address;

        try
        {
            // Determines the network address of the machine that runs the WRES
            // Should be of the format of '192.168.122.1'
            address = NetworkInterface.getNetworkInterfaces()
                                      .nextElement()
                                      .getInterfaceAddresses().get( 0 )
                                      .getAddress()
                                      .getHostName();
        }
        catch (SocketException e)
        {
            LOGGER.warn( "The execution address could not be determined.", e );
            address = "Unknown";
        }

        try
        {
            String username = SystemSettings.getUserName();
            Timestamp startTimestamp = new Timestamp( start );
            String runTime = duration + " MILLISECONDS";
            boolean failed = Strings.hasValue(error);

            // For any arguments that happen to be regular files, read the
            // contents of the first file into the "project" field. Maybe there
            // is an improvement that can be made, but this should cover the
            // common case of a single file in the args.
            String project = "";

            for ( String arg : arguments )
            {
                Path path = Paths.get( arg );

                if ( path.toFile()
                         .isFile() )
                {
                    try
                    {
                        project = String.join( System.lineSeparator(), Files.readAllLines( path ) );
                    }
                    catch ( IOException e )
                    {
                        LOGGER.warn( "A project could not be recorded." );
                    }

                    // Since this is an xml column, only go for first file.
                    break;
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
            script.addTab().addLine("'',");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("CAST(? AS INTERVAL),");
            script.addTab().addLine("?,");
            script.addTab().addLine("?");
            script.addLine(");");

            script.execute( String.join(" ", arguments),
                          project,
                          username,
                          address,
                          startTimestamp,
                          runTime,
                          failed,
                          error );
        }
        catch ( SQLException e )
        {
            LOGGER.warn("Execution metadata could not be logged to the database.");
        }
    }

    /**
     * Creates a set of {@link wres.config.generated.Feature Features} to
     * evaluate statistics for
     * @param projectDetails The object that holds details on what a project
     *                       should do
     * @return A set of {@link wres.config.generated.Feature Features}
     * @throws SQLException Thrown if information about the features could not
     * be retrieved from the database
     * @throws IOException Thrown if IO operations prevented the set from being
     * created
     */
    public static Set<Feature> decomposeFeatures( ProjectDetails projectDetails )

            throws SQLException, IOException
    {
        Set<Feature> atomicFeatures = new TreeSet<>( Comparator.comparing(
                ConfigHelper::getFeatureDescription ));

        for (FeatureDetails details : projectDetails.getFeatures())
        {
            // Check if the feature has any intersecting values
            Feature feature = details.toFeature();

            if ( projectDetails.getLeadOffset( feature ) != null)
            {
                atomicFeatures.add(details.toFeature());
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
        for ( IngestResult ingestResult : ingestResults )
        {
            if ( !ingestResult.wasFoundAlready() )
            {
                return true;
            }
        }

        return false;
    }

}
