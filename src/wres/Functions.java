package wres;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

import wres.config.MultiDeclarationFactory;
import wres.config.xml.ProjectConfigPlus;
import wres.config.xml.ProjectConfigs;
import wres.config.yaml.DeclarationException;
import wres.config.yaml.DeclarationFactory;
import wres.config.yaml.DeclarationMigrator;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.eventsbroker.embedded.EmbeddedBroker;
import wres.io.database.ConnectionSupplier;
import wres.io.database.caching.DatabaseCaches;
import wres.io.reading.netcdf.grid.GriddedFeatures;
import wres.io.database.Database;
import wres.io.database.DatabaseOperations;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.SourceLoader;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.writing.netcdf.NetCDFCopier;
import wres.pipeline.Canceller;
import wres.pipeline.Evaluator;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.io.database.locking.DatabaseLockManager;
import wres.server.WebServer;
import wres.system.DatabaseType;
import wres.system.SystemSettings;

/**
 * General purpose API for core wres functions.
 * If you want these to be accessible from a standalone deploy then you will need to add an entry in
 * {@link wres.helpers.MainUtilities}.
 *
 * @author Chris Tubbs
 * @author James Brown
 * @author Evan Pagryzinski
 */
public final class Functions
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Functions.class );

    /** Default port for the local server. */
    private static final String DEFAULT_PORT = "8010";

    /** Database. */
    private static Database database;

    /** Within-process broker for statistics messaging. */
    private static EmbeddedBroker broker;

    /** Connections for the statistics message broker. */
    private static BrokerConnectionFactory brokerConnectionFactory;

    /**
     * Executes an evaluation.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult evaluate( SharedResources sharedResources )
    {
        return Functions.evaluate( sharedResources, Canceller.of() );
    }

    /**
     * Executes an evaluation.
     * @param sharedResources the shared resources
     * @return the execution result
     */
    public static ExecutionResult evaluate( SharedResources sharedResources, Canceller canceller )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            setupConnections( sharedResources );
            Evaluator evaluator = new Evaluator( sharedResources.systemSettings,
                                                 database,
                                                 brokerConnectionFactory );

            List<String> args = sharedResources.arguments();
            if ( args.size() != 1 )
            {
                String message = "Please supply a (single) path to a project declaration file to evaluate, like this: "
                                 + "bin/wres.bat execute c:/path/to/evaluation.yml";
                LOGGER.error( message );
                UserInputException e = new UserInputException( message );
                result = ExecutionResult.failure( e, false ); // Or return 400 - Bad Request (see #41467)
                return result;
            }

            // Attempt to read the declaration
            String declarationOrPath = args.get( 0 )
                                           .trim();


            result = evaluator.evaluate( declarationOrPath, canceller );
            return result;
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
            Functions.teardownConnections();
        }
    }

    /**
     * Connects to the database.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult connectToDatabase( SharedResources sharedResources )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            setupConnections( sharedResources );
            DatabaseOperations.testDatabaseConnection( database );

            result = ExecutionResult.success();
            return result;
        }
        catch ( SQLException | IOException se )
        {
            String message = "Could not connect to database.";
            LOGGER.warn( message, se );
            InternalWresException e = new InternalWresException( message );
            result = ExecutionResult.failure( e, false );
            return result;
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
            Functions.teardownConnections();
        }
    }

    /**
     * Cleans the database.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult cleanDatabase( SharedResources sharedResources )
    {
        if ( !sharedResources.systemSettings()
                             .isUseDatabase() )
        {
            throw new IllegalArgumentException(
                    "This is an in-memory execution. Cannot clean a database because there "
                    + "is no database to clean." );
        }

        try
        {
            Instant start = Instant.now();
            Functions.setupConnections( sharedResources );

            DatabaseLockManager lockManager =
                    DatabaseLockManager.from( sharedResources.systemSettings(),
                                              () -> database.getRawConnection() );

            try
            {
                lockManager.lockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                DatabaseOperations.cleanDatabase( database );
                lockManager.unlockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            }
            catch ( SQLException se )
            {
                String message = "Failed to clean the database.";
                LOGGER.error( message, se );
                InternalWresException e = new InternalWresException( message, se );
                logResults( sharedResources, ExecutionResult.failure( e, false ), start );
                return ExecutionResult.failure( e, false );
            }
            finally
            {
                lockManager.shutdown();
            }

            Functions.logResults( sharedResources, ExecutionResult.success(), start );
            return ExecutionResult.success();
        }
        finally
        {
            Functions.teardownConnections();
        }
    }

    /**
     * Migrates the database.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult migrateDatabase( SharedResources sharedResources )
    {
        if ( !sharedResources.systemSettings()
                             .isUseDatabase() )
        {
            throw new IllegalArgumentException(
                    "This is an in-memory execution. Cannot migrate a database because there "
                    + "is no database to migrate." );
        }

        try
        {
            Instant start = Instant.now();
            setupConnections( sharedResources );

            try
            {
                //The migrateDatabase method deals with database locking, so we don't need to worry about that here
                DatabaseOperations.migrateDatabase( database );
            }
            catch ( SQLException se )
            {
                String message = "Failed to migrate the database.";
                LOGGER.error( message, se );
                InternalWresException e = new InternalWresException( message, se );
                logResults( sharedResources, ExecutionResult.failure( e, false ), start );
                return ExecutionResult.failure( e, false );
            }

            Functions.logResults( sharedResources, ExecutionResult.success(), start );
            return ExecutionResult.success();
        }
        finally
        {
            Functions.teardownConnections();
        }
    }

    /**
     * Validates a NetCDF grid for ingest.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult validateNetcdfGrid( SharedResources sharedResources )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            List<String> args = sharedResources.arguments();

            String path = args.get( 0 );
            String variableName = args.get( 1 );

            try ( NetcdfDataset dataset = NetcdfDatasets.openDataset( path );
                  GridDataset grid = new GridDataset( dataset ) )
            {
                GridDatatype variable = grid.findGridDatatype( variableName );

                if ( variable == null )
                {
                    String message = "The given variable is not a valid projected grid variable.";
                    UserInputException e = new UserInputException( message );
                    LOGGER.error( message );
                    result = ExecutionResult.failure( e, false );
                    return result;
                }
                else
                {
                    GridCoordSystem coordSystem = variable.getCoordinateSystem();

                    if ( coordSystem != null )
                    {
                        result = ExecutionResult.success();
                        return result;
                    }

                    String message = "The given coordinate system is not valid.";
                    result = ExecutionResult.failure( new UserInputException( message ), false );
                    return result;
                }
            }
            catch ( IOException e )
            {
                String message = "The file at '" + path
                                 + "' is not a valid Netcdf Grid Dataset.";
                UserInputException uie = new UserInputException( message, e );
                LOGGER.error( message );
                result = ExecutionResult.failure( uie, false );
                return result;
            }
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
        }
    }

    /**
     * Refreshes the core application database, where applicable.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult refreshDatabase( final SharedResources sharedResources )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            setupConnections( sharedResources );
            DatabaseLockManager lockManager =
                    DatabaseLockManager.from( sharedResources.systemSettings(),
                                              () -> database.getRawConnection() );

            try
            {
                lockManager.lockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                DatabaseOperations.refreshDatabase( database );
                lockManager.unlockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                result = ExecutionResult.success();
                return result;
            }
            catch ( SQLException | RuntimeException e )
            {
                LOGGER.error( "refreshDatabase failed.", e );
                result = ExecutionResult.failure( e, false );
                return result;
            }
            finally
            {
                lockManager.shutdown();
            }
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
            Functions.teardownConnections();
        }
    }

    /**
     * Starts a long-running server.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult startServer( final SharedResources sharedResources )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            List<String> args = sharedResources.arguments();

            if ( args.size() > 2 )
            {
                String message = """
                        Too many arguments provided. Please use this command in the following ways:
                        bin/wres.bat server <PORT_OVERRIDE>
                        bin/wres.bat server
                        """;
                LOGGER.error( message );
                UserInputException e = new UserInputException( message );
                result = ExecutionResult.failure( e, false ); // Or return 400 - Bad Request (see #41467)
                return result;
            }

            String port = DEFAULT_PORT;
            if ( args.size() == 1 )
            {
                String message =
                        "Starting server on default port 8010. An alternative port can be provided like this: "
                        + "bin/wres.bat server 8010";
                LOGGER.info( message );
            }
            else
            {
                // Attempt to read the port
                port = args.get( 1 ).trim();
                LOGGER.info( "Starting server on port: {}", port );
            }

            try
            {
                WebServer.main( new String[] { port } );
            }
            catch ( Exception e )
            {
                LOGGER.error( "Failed to start the server", e );
                result = ExecutionResult.failure( e, false );
                return result;
            }

            result = ExecutionResult.success();
            return result;
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
        }
    }

    /**
     * Performs ingest of datasets into the core application database.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult ingest( SharedResources sharedResources )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            setupConnections( sharedResources );
            List<String> args = sharedResources.arguments();

            if ( !args.isEmpty() )
            {
                String projectPath = args.get( 0 );
                String declarationString;
                EvaluationDeclaration declaration;

                try
                {
                    FileSystem fileSystem = FileSystems.getDefault();
                    declarationString = MultiDeclarationFactory.getDeclarationString( projectPath, fileSystem );
                    declaration = MultiDeclarationFactory.from( declarationString, fileSystem, true, true );
                }
                catch ( IOException ioe )
                {
                    Exception e = new PreIngestException( "Could not read declaration from "
                                                          + projectPath,
                                                          ioe );
                    return ExecutionResult.failure( e, false );
                }

                DatabaseLockManager lockManager =
                        DatabaseLockManager.from( sharedResources.systemSettings(),
                                                  () -> database.getRawConnection() );

                // Executors to close
                ExecutorService readingExecutor = null;
                ExecutorService ingestExecutor = null;

                try
                {
                    lockManager.lockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );

                    // Build the database caches/ORMs
                    SystemSettings settings = sharedResources.systemSettings();
                    DatabaseCaches caches = DatabaseCaches.of( database );

                    // Create a reading executor
                    // Inner readers may create additional thread factories (e.g., archives).
                    ThreadFactory readingFactory = new BasicThreadFactory.Builder()
                            .namingPattern( "Outer Reading Thread %d" )
                            .build();
                    BlockingQueue<Runnable> readingQueue = new ArrayBlockingQueue<>( 100_000 );

                    // Create some thread pools to perform the work required by different parts of the evaluation pipeline

                    // Thread pool for reading formats
                    RejectedExecutionHandler readingHandler = new ThreadPoolExecutor.CallerRunsPolicy();
                    readingExecutor = new ThreadPoolExecutor( settings.getMaximumReadThreads(),
                                                              settings.getMaximumReadThreads(),
                                                              settings.getPoolObjectLifespan(),
                                                              TimeUnit.MILLISECONDS,
                                                              readingQueue,
                                                              readingFactory,
                                                              readingHandler );

                    // Create an ingest executor
                    ThreadFactory ingestFactory =
                            new BasicThreadFactory.Builder().namingPattern( "Ingesting Thread %d" )
                                                            .build();
                    // Queue should be large enough to allow join() call to be reached with zero or few rejected submissions to the
                    // executor service.
                    BlockingQueue<Runnable> ingestQueue =
                            new ArrayBlockingQueue<>( settings.getMaximumIngestThreads() );

                    RejectedExecutionHandler ingestHandler = new ThreadPoolExecutor.CallerRunsPolicy();
                    ingestExecutor = new ThreadPoolExecutor( settings.getMaximumIngestThreads(),
                                                             settings.getMaximumIngestThreads(),
                                                             settings.getPoolObjectLifespan(),
                                                             TimeUnit.MILLISECONDS,
                                                             ingestQueue,
                                                             ingestFactory,
                                                             ingestHandler );

                    TimeSeriesIngester timeSeriesIngester =
                            new DatabaseTimeSeriesIngester.Builder().setSystemSettings( settings )
                                                                    .setDatabase( database )
                                                                    .setCaches( caches )
                                                                    .setIngestExecutor( ingestExecutor )
                                                                    .setLockManager( lockManager )
                                                                    .build();

                    // Gridded ingest is a special snowflake for now. See #51232
                    GriddedFeatures.Builder griddedFeatures = null;

                    if ( Objects.nonNull( declaration.spatialMask() ) )
                    {
                        griddedFeatures = new GriddedFeatures.Builder( declaration.spatialMask() );
                    }

                    SourceLoader.load( timeSeriesIngester,
                                       sharedResources.systemSettings(),
                                       declaration,
                                       griddedFeatures,
                                       readingExecutor );

                    lockManager.unlockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                    result = ExecutionResult.success( declaration.label(), declarationString );
                    return result;
                }
                catch ( RuntimeException | SQLException e )
                {
                    LOGGER.error( "Failed to ingest from {}", projectPath, e );
                    result = ExecutionResult.failure( declaration.label(), declarationString, e, false );
                    return result;
                }
                finally
                {
                    lockManager.shutdown();
                    Canceller.closeGracefully( readingExecutor );
                    Canceller.closeGracefully( ingestExecutor );
                }
            }
            else
            {
                String message = "There are not enough arguments to run 'ingest'"
                                 + System.lineSeparator()
                                 + "usage: ingest <path to configuration>";
                IllegalArgumentException e = new IllegalArgumentException( message );
                LOGGER.error( message );
                result = ExecutionResult.failure( e, false );
                return result;
            }
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
            Functions.teardownConnections();
        }
    }

    /**
     * Validates the project declaration.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult validate( SharedResources sharedResources )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            List<String> args = sharedResources.arguments();

            LOGGER.info( "Proceeding to validate a project declaration..." );

            if ( !args.isEmpty() )
            {
                String pathOrDeclaration = args.get( 0 );
                String rawDeclaration = null;
                try
                {

                    FileSystem fileSystem = FileSystems.getDefault();
                    rawDeclaration = MultiDeclarationFactory.getDeclarationString( pathOrDeclaration, fileSystem );

                    // Unmarshal the declaration
                    MultiDeclarationFactory.from( rawDeclaration,
                                                  fileSystem,
                                                  true,
                                                  true );

                    LOGGER.info( "The supplied declaration is valid: '{}'.", pathOrDeclaration );
                    result = ExecutionResult.success();
                    return result;
                }
                catch ( IOException | DeclarationException error )
                {
                    LOGGER.warn( "The supplied declaration is invalid.", error );
                    String message = "Failed to unmarshal and validate the project declaration at '"
                                     + pathOrDeclaration
                                     + "'";
                    UserInputException e = new UserInputException( message, error );
                    result = ExecutionResult.failure( rawDeclaration,
                                                    e,
                                                    false ); // Or return 400 - Bad Request (see #41467)
                    return result;
                }
            }
            else
            {
                String message = "Could not find a project declaration to validate. Usage: validate <path to project>";
                LOGGER.error( message );
                UserInputException e = new UserInputException( message );
                result = ExecutionResult.failure( e, false );
                return result;
            }
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
        }
    }

    /**
     * Migrates an old-style XML declaration to a new-style YAML declaration.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult migrate( SharedResources sharedResources )
    {
        return Functions.migrate( sharedResources, false );
    }

    /**
     * Migrates an old-style XML declaration to a new-style YAML declaration. If the declaration includes any external
     * sources of CSV thresholds, they will be migrated inline to the declaration.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    public static ExecutionResult migrateInline( SharedResources sharedResources )
    {
        return Functions.migrate( sharedResources, true );
    }

    /**
     * Migrates an old-style XML declaration to a new-style YAML declaration.
     * @param sharedResources the shared resources
     * @param inlineThresholds is true to migrate any external CSV thresholds inline to the declaration
     * @return the execution result
     */

    public static ExecutionResult migrate( SharedResources sharedResources, boolean inlineThresholds )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            List<String> args = sharedResources.arguments();

            if ( !args.isEmpty() )
            {
                String evaluationConfigArgument = args.get( 0 )
                                                      .trim();
                try
                {
                    ProjectConfigPlus projectConfigPlus = ProjectConfigs.readDeclaration( evaluationConfigArgument );
                    EvaluationDeclaration newDeclaration =
                            DeclarationMigrator.from( projectConfigPlus.getProjectConfig(), inlineThresholds );
                    LOGGER.debug( "Migrated the supplied declaration to: {}.", newDeclaration );
                    String yaml = DeclarationFactory.from( newDeclaration );
                    String announce = "Here is your migrated declaration:";
                    String start =
                            "---"; // Start of a YAML document. Not needed or returned, in general, but clean here

                    // Log if possible
                    if ( LOGGER.isInfoEnabled() )
                    {
                        LOGGER.info( "{}{}{}{}{}",
                                     announce,
                                     System.lineSeparator(),
                                     start,
                                     System.lineSeparator(),
                                     yaml );
                    }
                    else
                    {
                        // This is intended behaviour, so disable SQ sqid S106
                        System.out.println( announce // NOSONAR
                                            + System.lineSeparator()
                                            + start
                                            + System.lineSeparator()
                                            + yaml );
                    }

                    result = ExecutionResult.success();
                    return result;
                }
                catch ( UserInputException | IOException e )
                {
                    LOGGER.error( "Failed to unmarshal project declaration from command line argument.", e );
                    result = ExecutionResult.failure( e, false );
                    return result;
                }
            }
            else
            {
                String message = "The declaration path or string was missing. Usage: validate "
                                 + "<path to declaration or string>";
                UserInputException e = new UserInputException( message );
                result = ExecutionResult.failure( e, false );
                return result;
            }
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
        }
    }

    /**
     * Creates a NetCDT template.
     * @param sharedResources the shared resources
     * @return the execution result
     */
    public static ExecutionResult createNetCDFTemplate( SharedResources sharedResources )
    {
        ExecutionResult result = ExecutionResult.failure();
        Instant startedExecution = Instant.now();
        try
        {
            List<String> args = sharedResources.arguments();

            if ( args.size() > 1 )
            {
                try
                {
                    String fromFileName = args.get( 0 );
                    String toFileName = args.get( 1 );

                    if ( !Files.exists( Paths.get( fromFileName ) ) )
                    {
                        Exception e = new IllegalArgumentException( "The source file '" +
                                                                    fromFileName
                                                                    +
                                                                    "' does not exist. A "
                                                                    + "template file must "
                                                                    + "have a valid source "
                                                                    + "file." );
                        result = ExecutionResult.failure( e, false );
                        return result;
                    }

                    if ( !toFileName.toLowerCase().endsWith( "nc" ) )
                    {
                        Exception e = new IllegalArgumentException( "The name for the "
                                                                    + "template is invalid; "
                                                                    + "it must end with "
                                                                    + "'*.nc' to indicate "
                                                                    + "that it is a Netcdf "
                                                                    + "file." );
                        result = ExecutionResult.failure( e, false );
                        return result;
                    }

                    try ( NetCDFCopier writer = new NetCDFCopier( fromFileName, toFileName, ZonedDateTime.now() ) )
                    {
                        writer.write();
                    }

                    result = ExecutionResult.success();
                    return result;
                }
                catch ( IOException | RuntimeException error )
                {
                    LOGGER.error( "createNetCDFTemplate failed.", error );
                    result = ExecutionResult.failure( error, false );
                    return result;
                }
            }
            else
            {
                String message = "There are not enough arguments to create a template."
                                 + System.lineSeparator()
                                 + "usage: createnetcdftemplate <path to original file> <path to template>";
                LOGGER.error( message );
                UserInputException e = new UserInputException( message );
                result = ExecutionResult.failure( e, false );
                return result;
            }
        }
        finally
        {
            Functions.logResults( sharedResources, result, startedExecution );
        }
    }

    /**
     * Method to establish needed connections when running a command needs access to the database
     * Must call teardownConnections() after the method is complete
     * @param sharedResources used to get the systemSettings associated with this command
     */
    private static void setupConnections( SharedResources sharedResources )
    {
        // If this complex operation involves a database, check that one exists
        if ( sharedResources.systemSettings.isUseDatabase() && ( Objects.isNull( database ) || database.isShutdown() ) )
        {
            database = new Database( new ConnectionSupplier( sharedResources.systemSettings ) );
            // Migrate the database, as needed
            if ( database.getAttemptToMigrate() )
            {
                try
                {
                    DatabaseOperations.migrateDatabase( database );
                }
                catch ( SQLException e )
                {
                    throw new IllegalStateException( "Failed to migrate the WRES database.", e );
                }
            }
        }

        if ( Objects.isNull( broker ) || !broker.isActive() )
        {
            // Create the broker connections for statistics messaging
            Properties brokerConnectionProperties =
                    BrokerUtilities.getBrokerConnectionProperties( BrokerConnectionFactory.DEFAULT_PROPERTIES );

            // Create an embedded broker for statistics messages, if needed
            if ( BrokerUtilities.isEmbeddedBrokerRequired( brokerConnectionProperties ) )
            {
                broker = EmbeddedBroker.of( brokerConnectionProperties, false );
            }

            Functions.brokerConnectionFactory = BrokerConnectionFactory.of( brokerConnectionProperties );
        }
    }

    /**
     * Method to tear down connections established for a command that needed access to the database.
     * Must be called if setupConnections is used
     */
    private static void teardownConnections()
    {
        if ( Objects.nonNull( database ) )
        {
            // #81660
            LOGGER.info( "Closing database activities..." );
            database.shutdown();
            LOGGER.info( "The database activities have been closed." );
        }

        if ( Objects.nonNull( Functions.broker ) )
        {
            try
            {
                Functions.broker.close();
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Failed to close the embedded broker.", e );
            }
        }
    }

    /**
     * Log the time an execution took
     * @param sharedResources object that contains execution details
     * @param startTime the time this execution was started at
     */
    private static void logResults( SharedResources sharedResources, Instant startTime )
    {
        // Log timing of execution
        if ( LOGGER.isInfoEnabled() )
        {
            Instant endedExecution = Instant.now();
            Duration duration = Duration.between( startTime, endedExecution );
            LOGGER.info( "The function '{}' took {}", sharedResources.operation, duration );
        }
    }

    /**
     * Logs the results of a complex execution into the database
     * @param sharedResources object used to contain the execution details
     * @param executionResult the results of the execution
     * @param startTime time the execution started, used for logging total execution time
     */
    private static void logResults( SharedResources sharedResources,
                                    ExecutionResult executionResult,
                                    Instant startTime )
    {
        logResults( sharedResources, startTime );

        // Log the execution to the database if a database is used
        if ( sharedResources.systemSettings.isUseDatabase() )
        {
            setupConnections( sharedResources );
            Instant endedExecution = Instant.now();
            // Log both the operation and the args
            List<String> argList = new ArrayList<>();
            argList.add( sharedResources.operation );
            argList.addAll( sharedResources.arguments );

            DatabaseOperations.LogParameters logParameters =
                    new DatabaseOperations.LogParameters( argList,
                                                          executionResult.getName(),
                                                          executionResult.getDeclaration(),
                                                          executionResult.getHash(),
                                                          startTime,
                                                          endedExecution,
                                                          executionResult.failed(),
                                                          executionResult.getException(),
                                                          Main.getVersion(),
                                                          executionResult.getEvaluationId() );

            DatabaseOperations.logExecution( database,
                                             logParameters );
            teardownConnections();
        }
    }

    /** Small value class of information for an execution */
    public record SharedResources( SystemSettings systemSettings,
                                   String operation,
                                   List<String> arguments )
    {
        /**
         * @param systemSettings the system settings, not null
         * @param arguments the arguments
         */
        public SharedResources
        {
            Objects.requireNonNull( systemSettings );
            Objects.requireNonNull( arguments );
        }
    }

    /** Do not construct. */
    private Functions()
    {
    }
}
