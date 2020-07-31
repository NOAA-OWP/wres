package wres;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.NetcdfFile;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;

import wres.config.ProjectConfigPlus;
import wres.config.Validation;
import wres.config.generated.ProjectConfig;
import wres.control.Control;
import wres.io.Operations;
import wres.io.concurrency.Executor;
import wres.io.config.ConfigHelper;
import wres.io.utilities.Database;
import wres.system.DatabaseConnectionSupplier;
import wres.system.DatabaseLockManager;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
final class MainFunctions
{
	static final Integer FAILURE = -1;
	static final Integer SUCCESS = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MainFunctions.class);

    private MainFunctions(){}

	// Mapping of String names to corresponding methods
    private static final Map<String, Function<SharedResources, Integer>> FUNCTIONS = createMap();

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
	 * @param args      The desired arguments to use when calling the method
	 */
    static Integer call (String operation, final SharedResources sharedResources )
    {
	    Integer result = FAILURE;
		operation = operation.toLowerCase();

		if (MainFunctions.hasOperation(operation))
		{
            result = FUNCTIONS.get(operation).apply( sharedResources );
        }

		return result;
	}

	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
    private static Map<String, Function<SharedResources, Integer>> createMap ()
    {
        final Map<String, Function<SharedResources, Integer>> functions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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
		functions.put("readheader", MainFunctions::readHeader);

		return functions;
	}

    private static Integer execute( SharedResources sharedResources )
    {
        try ( Control control = new Control( sharedResources.getSystemSettings(),
                                             sharedResources.getDatabase(),
                                             sharedResources.getExecutor() ); )
        {
            return control.apply( sharedResources.getArguments() );
        }
    }

    /**
	 * Creates the "print_commands" method
	 *
	 * @return Method that prints all available commands by name
	 */
    private static Integer printCommands( final SharedResources sharedResources )
	{
		LOGGER.info("Available commands are:");
        for (final String command : FUNCTIONS.keySet())
        {
            LOGGER.info("\t{}", command);
        }
        return SUCCESS;
	}

	/**
	 * Creates the "connectToDB" method
	 *
	 * @return method that will attempt to connect to the database to prove that a connection is possible. The version of the connected database will be printed.
	 */
    private static Integer connectToDB(final SharedResources sharedResources ){

        try
        {
            Operations.testConnection( sharedResources.getDatabase() );
            return SUCCESS;
        }
        catch ( SQLException se )
        {
            LOGGER.warn( "Could not connect to database.", se );
            return FAILURE;
        }
	}

	/**
	 * Creates the "cleanDatabase" method
	 *
	 * @return A method that will remove all dynamic forecast, observation, and variable data from the database. Prepares the
	 * database for a cold start.
	 */
    private static Integer cleanDatabase( final SharedResources sharedResources )
	{
        Integer result;
        Supplier<Connection> connectionSupplier =
                new DatabaseConnectionSupplier( sharedResources.getSystemSettings() );
        DatabaseLockManager lockManager = new DatabaseLockManager( connectionSupplier );

        try
        {
            lockManager.lockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            Operations.cleanDatabase( sharedResources.getDatabase() );
            result = SUCCESS;
            lockManager.unlockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch ( SQLException e )
        {
            LOGGER.error( "While cleaning the database", e );
            MainFunctions.addException( e );
            result = FAILURE;
        }
        finally
        {
            lockManager.shutdown();
        }

        return result;
	}

    private static Integer validateNetcdfGrid( SharedResources sharedResources )
    {
        Integer result = FAILURE;
        String[] args = sharedResources.getArguments();

        String path = args[0];
        String variableName = args[1];

        try ( NetcdfDataset dataset = NetcdfDataset.openDataset( path ); GridDataset grid = new GridDataset( dataset ) )
        {
            GridDatatype variable = grid.findGridDatatype( variableName );

            if ( variable == null )
            {
                LOGGER.error( "The given variable is not a valid projected grid variable." );
            }
            else
            {
                GridCoordSystem coordSystem = variable.getCoordinateSystem();

                if ( coordSystem != null )
                {
                    result = SUCCESS;
                }
            }
        }
        catch ( IOException e )
        {
            LOGGER.error( "The file at {} is not a valid Netcdf Grid Dataset.", path );
            MainFunctions.addException( e );
        }

        return result;
    }

    private static Integer refreshDatabase( final SharedResources sharedResources )
	{
        Integer result = FAILURE;
        Supplier<Connection> connectionSupplier =
                new DatabaseConnectionSupplier( sharedResources.getSystemSettings() );
        DatabaseLockManager lockManager = new DatabaseLockManager( connectionSupplier );

        try
        {
            lockManager.lockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            Operations.refreshDatabase( sharedResources.getDatabase() );
            result = SUCCESS;
            lockManager.unlockExclusive( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
        }
        catch (final Exception e)
        {
            MainFunctions.addException( e );
            LOGGER.error(Strings.getStackTrace(e));
        }
        finally
        {
            lockManager.shutdown();
        }

        return result;
	}

    private static Integer ingest( SharedResources sharedResources )
    {
        int result = FAILURE;
        String[] args = sharedResources.getArguments();

        if ( args.length > 0 )
        {
            String projectPath = args[0];

            ProjectConfig projectConfig;
            Supplier<Connection> connectionSupplier =
                    new DatabaseConnectionSupplier( sharedResources.getSystemSettings() );
            DatabaseLockManager lockManager = new DatabaseLockManager( connectionSupplier );

            try
            {
                lockManager.lockShared( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                projectConfig = ConfigHelper.read(projectPath);
                Operations.ingest( sharedResources.getSystemSettings(),
                                   sharedResources.getDatabase(),
                                   sharedResources.getExecutor(),
                                   projectConfig,
                                   lockManager );
                result = SUCCESS;
                lockManager.unlockShared( DatabaseLockManager.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            }
            catch ( IOException | SQLException e )
            {
                MainFunctions.addException( e );
                LOGGER.error(Strings.getStackTrace(e));
            }
            finally
            {
                lockManager.shutdown();
            }
        }
        else
        {
            LOGGER.error("There are not enough arguments to run 'ingest'");
            LOGGER.error("usage: ingest <path to configuration>");
        }

        return result;
    }

    private static Integer validate( SharedResources sharedResources )
    {
        int result = FAILURE;
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
                LOGGER.error("Failed to unmarshal the project configuration at '" + fullPath + "'");
                LOGGER.error(Strings.getStackTrace( ioe ));
                MainFunctions.addException( ioe );
                return result; // Or return 400 - Bad Request (see #41467)
            }

            SystemSettings systemSettings = SystemSettings.fromDefaultClasspathXmlFile();

            // Validate unmarshalled configurations
            final boolean validated =
                    Validation.isProjectValid( systemSettings, projectConfigPlus );

            if ( validated )
            {
                LOGGER.info("'" + fullPath + "' is a valid project config.");
                result = SUCCESS;
            }
            else
            {
                // Even though the application performed its job, we still want
                // to return a failure so that the return code may be used to
                // determine the validity
                LOGGER.info("'" + fullPath + "' is not a valid config.");
            }

        }
        else
        {
            LOGGER.error( "A project path was not passed in" );
            LOGGER.error("usage: validate <path to project>");
        }

        return result;
    }

    private static Integer createNetCDFTemplate( SharedResources sharedResources )
    {
        int result = FAILURE;
        String[] args = sharedResources.getArguments();

        if (args.length > 1)
        {
            try
            {
                String fromFileName = args[0];
                String toFileName = args[1];

                if (!Files.exists( Paths.get( fromFileName ) ))
                {
                    throw new IllegalArgumentException( "The source file '" +
                                                        fromFileName +
                                                        "' does not exist. A "
                                                        + "template file must "
                                                        + "have a valid source "
                                                        + "file." );
                }

                if (!toFileName.toLowerCase().endsWith( "nc" ))
                {
                    throw new IllegalArgumentException( "The name for the "
                                                        + "template is invalid; "
                                                        + "it must end with "
                                                        + "'*.nc' to indicate "
                                                        + "that it is a NetCDF "
                                                        + "file." );
                }

                Operations.createNetCDFOutputTemplate( fromFileName, toFileName );
                result = SUCCESS;
            }
            catch (IOException | RuntimeException error)
            {
                MainFunctions.addException( error );
                LOGGER.error(Strings.getStackTrace( error ));
            }
        }
        else
        {
            LOGGER.error("There are not enough arguments to create a template.");
            LOGGER.error("usage: createnetcdftemplate <path to original file> <path to template>");
        }

        return result;
    }

    private static Integer readHeader( SharedResources sharedResources )
    {
        Integer result = FAILURE;
        final String url = "http://***REMOVED***rgw.***REMOVED***.***REMOVED***:8080/nwm/nwm.20180515/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc";

        // TODO: Finish experiment; current work must be halted

        /*
         * The idea here is that we want to see if we can pull the top x
         * bytes from the remote file. From there, we can pull and work with
         * just the header of a netcdf file.  If the data within the file
         * meets expectations, we may continue to work with it. If not,
         * we can avoid further work. If we can't avoid it, we are forced
         * to download a full file before we can determine whether or not
         * it can/should be used.
         */

        // Create a connection and open a stream to the resource
        URL sourcePath = null;
        try
        {
            sourcePath = new URL( url);
        }
        catch ( MalformedURLException e )
        {
            LOGGER.error("The url pointing towards the file was incorrect.", e);
            return FAILURE;
        }

        try(InputStream stream = sourcePath.openStream())
        {
            // The idea of 200 bytes for the header was pushed.
            // Working with 300 for initial tests just to be safe
            byte[] buffer = new byte[300];

            // Fill the buffer and attempt to load it into a Netcdf object
            // This is supposed to work in Thredds; it's called
            // "Range Subsetting"
            int bytesRead = stream.read( buffer );
            try (NetcdfFile data = NetcdfFile.openInMemory( "file", buffer ))
            {
                LOGGER.info( "Loaded data..." );
            }

            // If/when we can load the header, we want to be able to
            // evaluate the contents of it without actually hitting the
            // data. This might mean that attributes storing single
            // value data also contained within variables might
            // see use again. While "time" and "reference_time" are
            // the canonical sources of time and issue time data,
            // We most likely won't be able to hit that data
            // with the header alone (or at least reliably).
            result = SUCCESS;
        }
        catch ( IOException e )
        {
            LOGGER.error("Remote Netcdf reading failed.", e);
        }

        return result;
    }

    private static final Object EXCEPTION_LOCK = new Object();
    private static List<Exception> encounteredExceptions;

    private static void addException(Exception recentException)
    {
        synchronized ( EXCEPTION_LOCK )
        {
            if (MainFunctions.encounteredExceptions == null)
            {
                MainFunctions.encounteredExceptions = new ArrayList<>(  );
            }

            MainFunctions.encounteredExceptions.add(recentException);
        }
    }

    static List<Exception> getEncounteredExceptions()
    {
        synchronized ( EXCEPTION_LOCK )
        {
            if (MainFunctions.encounteredExceptions == null)
            {
                MainFunctions.encounteredExceptions = new ArrayList<>(  );
            }

            MainFunctions.encounteredExceptions.addAll( Control.getMostRecentException() );

            return Collections.unmodifiableList(MainFunctions.encounteredExceptions);
        }
    }

    static final class SharedResources
    {
        private final SystemSettings systemSettings;
        private final Database database;
        private final Executor executor;
        private final String[] arguments;

        public SharedResources( SystemSettings systemSettings,
                                Database database,
                                Executor executor,
                                String[] arguments )
        {
            Objects.requireNonNull( systemSettings );
            Objects.requireNonNull( database );
            Objects.requireNonNull( executor );
            Objects.requireNonNull( arguments );
            this.systemSettings = systemSettings;
            this.database = database;
            this.executor = executor;
            this.arguments = arguments;
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
    }
}
