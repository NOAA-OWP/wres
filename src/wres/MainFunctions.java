package wres;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dataset.NetcdfDatasets;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

import wres.config.ProjectConfigPlus;
import wres.config.Validation;
import wres.config.generated.ProjectConfig;
import wres.eventsbroker.BrokerConnectionFactory;
import wres.io.Operations;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.reading.PreIngestException;
import wres.io.utilities.Database;
import wres.pipeline.Evaluator;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

/**
 * @author ctubbs
 *
 */
final class MainFunctions
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MainFunctions.class);

    private MainFunctions(){}

	// Mapping of String names to corresponding methods
    private static final Map<String, Function<SharedResources, ExecutionResult>> FUNCTIONS = createMap();

    static void shutdown( Database database, Executor executor )
	{
	    ProgressMonitor.deactivate();
	    LOGGER.info("");
		LOGGER.info("Shutting down the application...");
        wres.io.Operations.shutdown( database, executor );
	}

    static void forceShutdown( Database database,
                               Executor executor,
                               long timeOut,
                               TimeUnit timeUnit )
    {
        ProgressMonitor.deactivate();
        LOGGER.info("");
        LOGGER.info( "Forcefully shutting down the application (you may see some errors)..." );
        wres.io.Operations.forceShutdown( database,
                                          executor,
                                          timeOut, timeUnit );
    }

	/**
	 * Determines if there is a method for the requested operation
	 *
	 * @param operation The desired operation to perform
	 * @return True if there is a method mapped to the operation name
	 */
	static boolean hasOperation (final String operation)
    {
		return FUNCTIONS.containsKey(operation.toLowerCase());
	}

	/**
	 * Executes the operation with the given list of arguments
	 *
	 * @param operation The name of the desired method to call
	 * @param sharedResources The resources required, including args.
	 */
    static ExecutionResult call (String operation, final SharedResources sharedResources )
    {
		operation = operation.toLowerCase();

		if (MainFunctions.hasOperation(operation))
		{
            return FUNCTIONS.get(operation).apply( sharedResources );
        }
		else
        {
            return ExecutionResult.failure( new UnsupportedOperationException( "Cannot find operation "
                                                                               + operation ) );
        }
	}

	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
    private static Map<String, Function<SharedResources, ExecutionResult>> createMap ()
    {
        final Map<String, Function<SharedResources, ExecutionResult>> functions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		functions.put("connecttodb", MainFunctions::connectToDB);
		functions.put("commands", MainFunctions::printCommands);
		functions.put("--help", MainFunctions::printCommands);
		functions.put("-h", MainFunctions::printCommands);
		functions.put("cleandatabase", MainFunctions::cleanDatabase);
        functions.put( "execute", MainFunctions::execute );
		functions.put("refreshdatabase", MainFunctions::refreshDatabase);
		functions.put("ingest", MainFunctions::ingest);
		functions.put("createnetcdftemplate", MainFunctions::createNetCDFTemplate);
		functions.put("validate", MainFunctions::validate);
		functions.put("validategrid", MainFunctions::validateNetcdfGrid);

		return functions;
	}

    private static ExecutionResult execute( SharedResources sharedResources )
    {
        Evaluator evaluator = new Evaluator( sharedResources.getSystemSettings(),
                                             sharedResources.getDatabase(),
                                             sharedResources.getExecutor(),
                                             sharedResources.getBrokerConnectionFactory() );

        return evaluator.evaluate( sharedResources.getArguments() );
    }

    /**
	 * Creates the "print_commands" method
	 *
	 * @return Method that prints all available commands by name
	 */
    private static ExecutionResult printCommands( final SharedResources sharedResources )
	{
		LOGGER.info("Available commands are:");
        for (final String command : FUNCTIONS.keySet())
        {
            LOGGER.info("\t{}", command);
        }

        return ExecutionResult.success();
	}

	/**
	 * Creates the "connectToDB" method
	 *
	 * @return method that will attempt to connect to the database to prove that a connection is possible. The version of the connected database will be printed.
	 */
    private static ExecutionResult connectToDB( SharedResources sharedResources )
    {
        try
        {
            Operations.testConnection( sharedResources.getDatabase() );
            return ExecutionResult.success();
        }
        catch ( SQLException se )
        {
            String message = "Could not connect to database.";
            LOGGER.warn( message, se );
            InternalWresException e = new InternalWresException( message );
            return ExecutionResult.failure( e );
        }
	}

	/**
	 * Creates the "cleanDatabase" method
	 *
	 * @return A method that will remove all dynamic forecast, observation, and variable data from the database. Prepares the
	 * database for a cold start.
	 */
    private static ExecutionResult cleanDatabase( SharedResources sharedResources )
	{
        DatabaseLockManager lockManager = DatabaseLockManager.from( sharedResources.getSystemSettings() );

        try
        {
            lockManager.lockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            Operations.cleanDatabase( sharedResources.getDatabase() );
            lockManager.unlockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( SQLException se )
        {
            String message = "Failed to clean the database.";
            LOGGER.error( message, se );
            InternalWresException e = new InternalWresException( message, se );
            return ExecutionResult.failure( null, e );
        }
        finally
        {
            lockManager.shutdown();
        }

        return ExecutionResult.success();
	}

    private static ExecutionResult validateNetcdfGrid( SharedResources sharedResources )
    {
        String[] args = sharedResources.getArguments();

        String path = args[0];
        String variableName = args[1];

        try ( NetcdfDataset dataset = NetcdfDatasets.openDataset( path );
              GridDataset grid = new GridDataset( dataset ) )
        {
            GridDatatype variable = grid.findGridDatatype( variableName );

            if ( variable == null )
            {
                String message = "The given variable is not a valid projected grid variable.";
                UserInputException e = new UserInputException( message );
                LOGGER.error( message );
                return ExecutionResult.failure( e );
            }
            else
            {
                GridCoordSystem coordSystem = variable.getCoordinateSystem();

                if ( coordSystem != null )
                {
                    return ExecutionResult.success();
                }

                String message = "The given coordinate system is not valid.";
                return ExecutionResult.failure( new UserInputException( message ) );
            }
        }
        catch ( IOException e )
        {
            String message = "The file at '" + path
                             + "' is not a valid Netcdf Grid Dataset.";
            UserInputException uie = new UserInputException( message, e );
            LOGGER.error( message );
            return ExecutionResult.failure( uie );
        }
    }

    private static ExecutionResult refreshDatabase( final SharedResources sharedResources )
	{
        DatabaseLockManager lockManager = DatabaseLockManager.from( sharedResources.getSystemSettings() );

        try
        {
            lockManager.lockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            Operations.refreshDatabase( sharedResources.getDatabase() );
            lockManager.unlockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            return ExecutionResult.success();
        }
        catch ( SQLException | RuntimeException e )
        {
            LOGGER.error( "refreshDatabase failed.", e );
            return ExecutionResult.failure( e );
        }
        finally
        {
            lockManager.shutdown();
        }
	}

    private static ExecutionResult ingest( SharedResources sharedResources )
    {
        String[] args = sharedResources.getArguments();

        if ( args.length > 0 )
        {
            String projectPath = args[0];
            ProjectConfig projectConfig;

            try
            {
                projectConfig = ConfigHelper.read( projectPath );
            }
            catch ( IOException ioe )
            {
                Exception e = new PreIngestException( "Could not read declaration from "
                                                      + projectPath, ioe );
                return ExecutionResult.failure( e );
            }

            DatabaseLockManager lockManager = DatabaseLockManager.from( sharedResources.getSystemSettings() );

            try
            {
                lockManager.lockShared( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                Operations.ingest( sharedResources.getSystemSettings(),
                                   sharedResources.getDatabase(),
                                   sharedResources.getExecutor(),
                                   projectConfig,
                                   lockManager );
                lockManager.unlockShared( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                return ExecutionResult.success( projectConfig.getName() );
            }
            catch ( RuntimeException | SQLException e )
            {
                LOGGER.error( "Failed to ingest from {}", projectPath, e );
                return ExecutionResult.failure( projectConfig.getName(), e );
            }
            finally
            {
                lockManager.shutdown();
            }
        }
        else
        {
            String message = "There are not enough arguments to run 'ingest'"
                             + System.lineSeparator()
                             + "usage: ingest <path to configuration>";
            IllegalArgumentException e = new IllegalArgumentException( message );
            LOGGER.error( message );
            return ExecutionResult.failure( e );
        }
    }

    private static ExecutionResult validate( SharedResources sharedResources )
    {
        String[] args = sharedResources.getArguments();

        if (args.length > 0)
        {
            Path configPath = Paths.get( args[0] );
            ProjectConfigPlus projectConfigPlus;

            String fullPath = configPath.toAbsolutePath().toString();

            try
            {
                // Unmarshal the configuration
                projectConfigPlus = ProjectConfigPlus.from( configPath );
            }
            catch ( IOException ioe )
            {
                String message = "Failed to unmarshal the project configuration at '" + fullPath + "'";
                UserInputException e = new UserInputException( message, ioe );
                LOGGER.error( "validate failed.", e );
                return ExecutionResult.failure( e ); // Or return 400 - Bad Request (see #41467)
            }

            SystemSettings systemSettings = SystemSettings.fromDefaultClasspathXmlFile();

            // Validate unmarshalled configurations
            final boolean validated =
                    Validation.isProjectValid( systemSettings, projectConfigPlus );

            if ( validated )
            {
                LOGGER.info("'" + fullPath + "' is a valid project config.");
                return ExecutionResult.success();
            }
            else
            {
                // Even though the application performed its job, we still want
                // to return a failure so that the return code may be used to
                // determine the validity
                String message = "'" + fullPath + "' is not a valid config.";
                LOGGER.info( message );
                UserInputException e = new UserInputException( message );
                return ExecutionResult.failure( e );
            }
        }
        else
        {
            String message = "A project path was not passed in"
                             + System.lineSeparator()
                             + "usage: validate <path to project>";
            LOGGER.error( message );
            UserInputException e = new UserInputException( message );
            return ExecutionResult.failure( e );
        }
    }

    private static ExecutionResult createNetCDFTemplate( SharedResources sharedResources )
    {
        String[] args = sharedResources.getArguments();

        if (args.length > 1)
        {
            try
            {
                String fromFileName = args[0];
                String toFileName = args[1];

                if (!Files.exists( Paths.get( fromFileName ) ))
                {
                    Exception e = new IllegalArgumentException( "The source file '" +
                                                        fromFileName +
                                                        "' does not exist. A "
                                                        + "template file must "
                                                        + "have a valid source "
                                                        + "file." );
                    return ExecutionResult.failure( e );
                }

                if (!toFileName.toLowerCase().endsWith( "nc" ))
                {
                    Exception e = new IllegalArgumentException( "The name for the "
                                                        + "template is invalid; "
                                                        + "it must end with "
                                                        + "'*.nc' to indicate "
                                                        + "that it is a NetCDF "
                                                        + "file." );
                    return ExecutionResult.failure( e );
                }

                Operations.createNetCDFOutputTemplate( fromFileName, toFileName );
                return ExecutionResult.success();
            }
            catch (IOException | RuntimeException error)
            {
                LOGGER.error( "createNetCDFTemplate failed.", error );
                return ExecutionResult.failure( error );
            }
        }
        else
        {
            String message = "There are not enough arguments to create a template."
                             + System.lineSeparator()
                             + "usage: createnetcdftemplate <path to original file> <path to template>";
            LOGGER.error( message );
            UserInputException e = new UserInputException( message );
            return ExecutionResult.failure( e );
        }
    }

    static final class SharedResources
    {
        private final SystemSettings systemSettings;
        private final Database database;
        private final Executor executor;
        private final BrokerConnectionFactory brokerConnectionFactory;
        private final String[] arguments;

        public SharedResources( SystemSettings systemSettings,
                                Database database,
                                Executor executor,
                                BrokerConnectionFactory brokerConnectionFactory,
                                String[] arguments )
        {
            Objects.requireNonNull( systemSettings );
            Objects.requireNonNull( database );
            Objects.requireNonNull( executor );
            Objects.requireNonNull( arguments );
            Objects.requireNonNull( brokerConnectionFactory );
            
            this.systemSettings = systemSettings;
            this.database = database;
            this.executor = executor;
            this.arguments = arguments;
            this.brokerConnectionFactory = brokerConnectionFactory;
        }

        public SystemSettings getSystemSettings()
        {
            return this.systemSettings;
        }

        public Database getDatabase()
        {
            return this.database;
        }

        public Executor getExecutor()
        {
            return this.executor;
        }

        public String[] getArguments()
        {
            return this.arguments;
        }
        
        public BrokerConnectionFactory getBrokerConnectionFactory()
        {
            return this.brokerConnectionFactory;
        }
    }
}
