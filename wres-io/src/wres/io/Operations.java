package wres.io;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
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
import wres.io.data.caching.Features;
import wres.io.data.caching.Projects;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.ProjectDetails;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.SourceLoader;
import wres.io.reading.fews.PIXMLReader;
import wres.io.retrieval.InputGenerator;
import wres.io.utilities.Database;
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

    public static void install()
    {
        Database.buildInstance();
    }

    public static void shutdown()
    {
        LOGGER.info("Shutting down the IO layer...");
        PairWriter.flushAndCloseAllWriters();
        Database.addNewIndexes();
        Executor.complete();
        Database.shutdown();
    }

    public static void shutdownWithAbandon( long timeOut, TimeUnit timeUnit )
    {
        LOGGER.info( "Forcefully shutting down the IO module..." );
        PairWriter.flushAndCloseAllWriters();
        Database.addNewIndexes();
        List<Runnable> executorTasks =
                Executor.shutdownWithAbandon( timeOut / 2, timeUnit );
        List<Runnable> databaseTasks =
                Database.shutdownWithAbandon( timeOut / 2, timeUnit );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "IO module was forcefully shut down. "
                         + "Abandoned around {} executor tasks and "
                         + "around {} database tasks.",
                         executorTasks.size(),
                         databaseTasks.size() );
        }
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
            Database.refreshStatistics( true );
            successfullyCleaned = SUCCESS;
        }
        catch (SQLException e)
        {
            LOGGER.error(Strings.getStackTrace(e));
        }
        return  successfullyCleaned;
    }

    public static boolean refreshDatabase()
    {
        Database.refreshStatistics(true);
        return SUCCESS;
    }

    public static void logExecution( String[] arguments,
                                     String start,
                                     String stop,
                                     boolean failed )
    {
        String address;

        try
        {
            address = String.valueOf(InetAddress.getLocalHost());
        }
        catch (UnknownHostException e)
        {
            LOGGER.warn( "Could not figure out host name", e );
            address = "Unknown";
        }

        try
        {
            String systemConfiguration = SystemSettings.getRawConfiguration();
            String username = SystemSettings.getUserName();

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
                    project = Operations.getFileContents( path );

                    // Since this is an xml column, only go for first file.
                    break;
                }
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
                            "'" + String.join( " ", arguments ) +
                            "', " +
                            "'" + systemConfiguration + "', " +
                            "'" + project + "', " +
                            "'" + username + "', " +
                            "'" + address + "', " +
                            "'" + start + "'::timestamp, " +
                            "'" + stop + "'::timestamp - '" + start
                            + "'::timestamp" +
                            ", " + failed + ");";

            Database.execute( script );
        }
        catch (XMLStreamException | TransformerException | IOException e)
        {
            LOGGER.warn("The system configuration could not be loaded. Execution information was not logged to the database.", e);
        }
        catch (SQLException e) {
            LOGGER.warn("Execution information could not be saved to the database.", e);
        }
    }

    /**
     * Return the contents of a file as a String
     * @param path a path that has already been verified as being a file
     * @return the contents of the file at path
     */

    private static String getFileContents( Path path )
    {
        StringJoiner project = new StringJoiner( System.lineSeparator() );

        try
        {
            for ( String line : Files.readAllLines( path ) )
            {
                project.add( line );
            }
        }
        catch ( IOException ioe )
        {
            LOGGER.warn( "While attempting to read path {} while logging executions",
                         path,
                         ioe );
        }

        return project.toString();
    }

    public static Set<Feature> decomposeFeatures( ProjectDetails projectDetails )

            throws SQLException, IOException
    {
        Set<Feature> atomicFeatures = new TreeSet<>( Comparator.comparing(
                ConfigHelper::getFeatureDescription ));

        for (FeatureDetails details : projectDetails.getFeatures())
        {
            // Check if the feature has any intersecting values
            Feature feature = details.toFeature();

            if ( projectDetails == null || projectDetails.getLeadOffset( feature ) != null)
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
        boolean shouldAnalyze = false;

        return shouldAnalyze;
    }

}
