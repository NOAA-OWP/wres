package wres;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
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
import wres.config.generated.UnnamedFeature;
import wres.events.broker.BrokerConnectionFactory;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DatabaseCaches;
import wres.io.data.caching.GriddedFeatures;
import wres.io.database.Database;
import wres.io.database.DatabaseOperations;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.writing.netcdf.NetCDFCopier;
import wres.pipeline.Evaluator;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseType;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;

/**
 * @author Chris Tubbs
 * @author James Brown
 */
final class Functions
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Functions.class );

    // Mapping of String names to corresponding methods
    private static final Map<WresFunction, Function<SharedResources, ExecutionResult>>
            FUNCTIONS_MAP = Functions.createMap();

    static void shutdown( Database database )
    {
        ProgressMonitor.deactivate();
        LOGGER.info( "Shutting down the application..." );
        Operations.shutdown( database );
    }

    static void forceShutdown( Database database,
                               long timeOut,
                               TimeUnit timeUnit )
    {
        ProgressMonitor.deactivate();
        LOGGER.info( "Forcefully shutting down the application (you may see some errors)..." );
        Operations.forceShutdown( database,
                                  timeOut,
                                  timeUnit );
    }

    /**
     * Determines if there is a method for the requested operation
     *
     * @param operation The desired operation to perform
     * @return True if there is a method mapped to the operation name
     */
    static boolean hasOperation( final String operation )
    {
        return FUNCTIONS_MAP.keySet()
                            .stream()
                            .anyMatch( next -> operation.equalsIgnoreCase( next.shortName )
                                               || operation.equalsIgnoreCase( next.longName ) );
    }

    /**
     * Executes the operation with the given list of arguments
     *
     * @param operation The name of the desired method to call
     * @param sharedResources The resources required, including args.
     */
    static ExecutionResult call( String operation, final SharedResources sharedResources )
    {
        operation = operation.toLowerCase();

        final String finalOperation = operation;
        Optional<Function<SharedResources, ExecutionResult>> function =
                FUNCTIONS_MAP.entrySet()
                             .stream()
                             .filter( next -> finalOperation.equals( next.getKey().shortName )
                                              || finalOperation.equals( next.getKey().longName ) )
                             .findFirst()
                             .map( Entry::getValue );

        if ( function.isPresent() )
        {
            return function.get()
                           .apply( sharedResources );
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
    private static Map<WresFunction, Function<SharedResources, ExecutionResult>> createMap()
    {
        final Map<WresFunction, Function<SharedResources, ExecutionResult>> functions = new TreeMap<>();

        functions.put( new WresFunction( "-cd", "connecttodb", "Connects to the WRES database." ),
                       Functions::connectToDatabase );
        functions.put( new WresFunction( "-co", "commands", "Prints this help information." ),
                       Functions::printCommands );
        functions.put( new WresFunction( "-h", "help", "Prints this help information." ),
                       Functions::printCommands );
        functions.put( new WresFunction( "-c", "cleandatabase", "Cleans the WRES database." ),
                       Functions::cleanDatabase );
        functions.put( new WresFunction( "-e",
                                         "execute",
                                         "Executes an evaluation with the declaration supplied as a path or string "
                                         + "(e.g., execute /foo/bar/project.xml)." ),
                       Functions::execute );
        functions.put( new WresFunction( "-r", "refreshdatabase", "Refreshes the database." ),
                       Functions::refreshDatabase );
        functions.put( new WresFunction( "-i",
                                         "ingest",
                                         "Ingests data supplied in a declaration path or string (e.g., ingest "
                                         + "/foo/bar/project.xml)." ),
                       Functions::ingest );
        functions.put( new WresFunction( "-cn",
                                         "createnetcdftemplate",
                                         "Creates a NetCDF template with the supplied NetCDF source (e.g., "
                                         + "createnetcdftemplate /foo/bar/source.nc)." ),
                       Functions::createNetCDFTemplate );
        functions.put( new WresFunction( "-v",
                                         "validate",
                                         "Validates the declaration supplied as a path or string (e.g., validate "
                                         + "/foo/bar/project.xml)." ),
                       Functions::validate );
        functions.put( new WresFunction( "-vg",
                                         "validategrid",
                                         "Validates a netcdf grid supplied as a path and a corresponding variable "
                                         + "name (e.g., validategrid /foo/bar/grid.nc baz)." ),
                       Functions::validateNetcdfGrid );

        return functions;
    }

    private static ExecutionResult execute( SharedResources sharedResources )
    {
        Evaluator evaluator = new Evaluator( sharedResources.systemSettings(),
                                             sharedResources.database(),
                                             sharedResources.brokerConnectionFactory() );

        return evaluator.evaluate( sharedResources.arguments() );
    }

    /**
     * Creates the "print_commands" method
     *
     * @return Method that prints all available commands by name
     */
    private static ExecutionResult printCommands( final SharedResources sharedResources )
    {
        LOGGER.info( "\tAvailable functions:" );
        for ( final WresFunction command : FUNCTIONS_MAP.keySet() )
        {
            LOGGER.info( "\t{}", command );
        }

        return ExecutionResult.success();
    }

    /**
     * Creates the "connectToDB" method
     *
     * @return method that will attempt to connect to the database to prove that a connection is possible. The version 
     * of the connected database will be printed.
     */
    private static ExecutionResult connectToDatabase( SharedResources sharedResources )
    {
        try
        {
            DatabaseOperations.testDatabaseConnection( sharedResources.database() );

            return ExecutionResult.success();
        }
        catch ( SQLException | IOException se )
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
     * @return A method that will remove all dynamic forecast, observation, and variable data from the database. 
     * Prepares the database for a cold start.
     */
    private static ExecutionResult cleanDatabase( SharedResources sharedResources )
    {
        if ( sharedResources.systemSettings()
                            .isInMemory() )
        {
            throw new IllegalArgumentException( "This is an in-memory execution. Cannot clean a database because there "
                                                + "is no database to clean." );
        }

        DatabaseLockManager lockManager =
                DatabaseLockManager.from( sharedResources.systemSettings(),
                                          () -> sharedResources.database()
                                                               .getRawConnection() );

        try
        {
            lockManager.lockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            DatabaseOperations.cleanDatabase( sharedResources.database() );
            lockManager.unlockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
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
        DatabaseLockManager lockManager =
                DatabaseLockManager.from( sharedResources.systemSettings(),
                                          () -> sharedResources.database()
                                                               .getRawConnection() );

        try
        {
            lockManager.lockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
            DatabaseOperations.refreshDatabase( sharedResources.database() );
            lockManager.unlockExclusive( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
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
        List<String> args = sharedResources.arguments();

        if ( !args.isEmpty() )
        {
            String projectPath = args.get( 0 );
            ProjectConfig projectConfig;

            try
            {
                projectConfig = ConfigHelper.read( projectPath );
            }
            catch ( IOException ioe )
            {
                Exception e = new PreIngestException( "Could not read declaration from "
                                                      + projectPath,
                                                      ioe );
                return ExecutionResult.failure( e );
            }

            DatabaseLockManager lockManager =
                    DatabaseLockManager.from( sharedResources.systemSettings(),
                                              () -> sharedResources.database()
                                                                   .getRawConnection() );

            try
            {
                lockManager.lockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );

                // Build the database caches/ORMs
                DatabaseCaches caches = DatabaseCaches.of( sharedResources.database(), projectConfig );
                TimeSeriesIngester timeSeriesIngester =
                        new DatabaseTimeSeriesIngester.Builder().setSystemSettings( sharedResources.systemSettings() )
                                                                .setDatabase( sharedResources.database() )
                                                                .setCaches( caches )
                                                                .setProjectConfig( projectConfig )
                                                                .setLockManager( lockManager )
                                                                .build();

                List<UnnamedFeature> gridSelection = projectConfig.getPair()
                                                                  .getGridSelection();

                // Gridded ingest is a special snowflake for now. See #51232
                GriddedFeatures.Builder griddedFeatures = null;

                if ( !gridSelection.isEmpty() )
                {
                    griddedFeatures = new GriddedFeatures.Builder( gridSelection );
                }

                Operations.ingest( timeSeriesIngester,
                                   sharedResources.systemSettings(),
                                   projectConfig,
                                   griddedFeatures );

                lockManager.unlockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
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
        List<String> args = sharedResources.arguments();

        if ( !args.isEmpty() )
        {
            Path configPath = Paths.get( args.get( 0 ) );
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
                LOGGER.info( "'{}' is a valid project config.", fullPath );
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
                    return ExecutionResult.failure( e );
                }

                if ( !toFileName.toLowerCase().endsWith( "nc" ) )
                {
                    Exception e = new IllegalArgumentException( "The name for the "
                                                                + "template is invalid; "
                                                                + "it must end with "
                                                                + "'*.nc' to indicate "
                                                                + "that it is a NetCDF "
                                                                + "file." );
                    return ExecutionResult.failure( e );
                }

                try ( NetCDFCopier writer = new NetCDFCopier( fromFileName, toFileName, ZonedDateTime.now() ) )
                {
                    writer.write();
                }

                return ExecutionResult.success();
            }
            catch ( IOException | RuntimeException error )
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

    /** Small value class of shared resources. */
    record SharedResources( SystemSettings systemSettings, Database database,
                            BrokerConnectionFactory brokerConnectionFactory, List<String> arguments )
    {
        /**
         * @param systemSettings the system settings, not null
         * @param database the database, optional if a database is not required
         * @param brokerConnectionFactory the broker connection factory
         * @param arguments the arguments
         */
        SharedResources
        {
            Objects.requireNonNull( systemSettings );
            Objects.requireNonNull( arguments );
            Objects.requireNonNull( brokerConnectionFactory );

            if ( systemSettings.isInDatabase() )
            {
                Objects.requireNonNull( database );
            }
        }
    }

    /** Small wrapper class for WRES functions. */
    private record WresFunction( String shortName, String longName, String description )
            implements Comparable<WresFunction>
    {
        @Override
        public String toString()
        {
            String returnMe = "";

            if ( Objects.nonNull( this.shortName ) )
            {
                returnMe = this.shortName;
                if ( Objects.isNull( this.longName ) )
                {
                    returnMe = StringUtils.rightPad( returnMe, 30 );
                }
                else
                {
                    returnMe = returnMe + ", ";
                }
            }

            if ( Objects.nonNull( this.longName ) )
            {
                returnMe = returnMe + this.longName;
                returnMe = StringUtils.rightPad( returnMe, 30 );
            }

            if ( Objects.nonNull( this.description ) )
            {
                returnMe = returnMe + this.description;
            }

            return returnMe;
        }

        @Override
        public int compareTo( WresFunction o )
        {
            Comparator<String> nullCompare = Comparator.nullsFirst( String::compareTo );
            Comparator<String> nullFriendly = ( left, right ) -> {
                if ( Objects.nonNull( left ) )
                {
                    left = left.toLowerCase();
                }

                if ( Objects.nonNull( right ) )
                {
                    right = right.toLowerCase();
                }

                return nullCompare.compare( left, right );
            };

            int compare = nullFriendly.compare( this.shortName, o.shortName );

            if ( compare != 0 )
            {
                return compare;
            }

            compare = nullFriendly.compare( this.longName, o.longName );

            if ( compare != 0 )
            {
                return compare;
            }

            return nullFriendly.compare( this.description, o.description );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !( o instanceof WresFunction in ) )
            {
                return false;
            }

            return Objects.equals( in.shortName, this.shortName ) && Objects.equals( in.longName, this.longName )
                   && Objects.equals( in.description, this.description );
        }
    }

    /** Do not construct. */
    private Functions()
    {
    }
}
