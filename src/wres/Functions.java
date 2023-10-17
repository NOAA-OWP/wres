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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
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
import wres.io.database.caching.DatabaseCaches;
import wres.io.reading.netcdf.grid.GriddedFeatures;
import wres.io.database.Database;
import wres.io.database.DatabaseOperations;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.SourceLoader;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.database.DatabaseTimeSeriesIngester;
import wres.io.writing.netcdf.NetCDFCopier;
import wres.pipeline.Evaluator;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.io.database.locking.DatabaseLockManager;
import wres.server.WebServer;
import wres.system.DatabaseType;
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

    /**
     * Determines if there is a method for the requested operation
     *
     * @param operation The desired operation to perform
     * @return True if there is a method mapped to the operation name
     */
    static boolean hasOperation( String operation )
    {
        Optional<Entry<WresFunction, Function<SharedResources, ExecutionResult>>> discovered
                = Functions.getOperation( operation );
        return discovered.isPresent();
    }

    /**
     * Inspects the operation and determines whether it is "simple" and, therefore, involves no spin-up or tear-down.
     * @param operation the operation name
     * @return whether the operation is simple
     */
    static boolean isSimpleOperation( String operation )
    {
        Optional<Entry<WresFunction, Function<SharedResources, ExecutionResult>>> discovered
                = Functions.getOperation( operation );
        return discovered.isPresent() && discovered.get()
                                                   .getKey()
                                                   .isSimpleOperation();
    }

    /**
     * Looks for a named operation.
     * @param operation the operation
     * @return an result that may contain the operation
     */
    private static Optional<Entry<WresFunction, Function<SharedResources, ExecutionResult>>> getOperation( String operation )
    {
        Objects.requireNonNull( operation );
        String finalOperation = operation.toLowerCase();
        return FUNCTIONS_MAP.entrySet()
                            .stream()
                            .filter( next -> finalOperation.equals( next.getKey().shortName() )
                                             || finalOperation.equals( next.getKey().longName() ) )
                            .findFirst();
    }

    /**
     * Executes the operation.
     *
     * @param operation The name of the desired method to call
     * @param sharedResources The resources required, including args.
     */
    static ExecutionResult call( String operation, SharedResources sharedResources )
    {
        // Execution began
        Instant beganExecution = Instant.now();

        // Log the operation
        if ( LOGGER.isInfoEnabled() )
        {
            StringJoiner joiner = new StringJoiner( " " );
            if ( !sharedResources.arguments()
                                 .contains( operation ) )
            {
                joiner.add( operation );
            }
            sharedResources.arguments()
                           .forEach( joiner::add );

            String report = Functions.curtail( joiner.toString() );

            LOGGER.info( "Executing: {}", report );
        }

        Optional<Entry<WresFunction, Function<SharedResources, ExecutionResult>>> discovered
                = Functions.getOperation( operation );

        ExecutionResult result;

        if ( discovered.isPresent() )
        {
            result = discovered.get()
                             .getValue()
                             .apply( sharedResources );
        }
        else
        {
            result = ExecutionResult.failure( new UnsupportedOperationException( "Cannot find operation "
                                                                               + operation ) );
        }

        // Log timing of execution
        if ( LOGGER.isInfoEnabled() )
        {
            Instant endedExecution = Instant.now();
            Duration duration = Duration.between( beganExecution, endedExecution );
            LOGGER.info( "The function '{}' took {}", operation, duration );
        }

        return result;
    }

    /**
     * Returns only the first line of the input string, appending "..." if more than one line was found.
     * @param input the input string
     * @return the first line
     */

    static String curtail( String input )
    {
        Objects.requireNonNull( input );

        // Only report the first line of a declaration string
        String[] split = input.split( "\\r?\\n|\\r" );
        String report = split[0];
        if ( split.length > 1 )
        {
            report = report + "...";
        }

        return report;
    }

    /**
     * Creates the mapping of operation names to their corresponding methods
     */
    private static Map<WresFunction, Function<SharedResources, ExecutionResult>> createMap()
    {
        // Map with insertion order
        Map<WresFunction, Function<SharedResources, ExecutionResult>> functions = new LinkedHashMap<>();

        functions.put( new WresFunction( "-c", "cleandatabase", "Cleans the WRES database.", false ),
                       Functions::cleanDatabase );
        functions.put( new WresFunction( "-cd", "connecttodb", "Connects to the WRES "
                                                               + "database.", false ),
                       Functions::connectToDatabase );
        functions.put( new WresFunction( "-co", "commands", "Prints this help information.", true ),
                       Functions::printCommands );
        functions.put( new WresFunction( "-cn",
                                         "createnetcdftemplate",
                                         "Creates a Netcdf template with the supplied Netcdf source. Example "
                                         + "usage: createnetcdftemplate /foo/bar/source.nc", true ),
                       Functions::createNetCDFTemplate );
        functions.put( new WresFunction( "-e",
                                         "execute",
                                         "Executes an evaluation with the declaration supplied as a path or "
                                         + "string. Example usage: execute /foo/bar/project.xml", false ),
                       Functions::execute );
        functions.put( new WresFunction( "-h", "help", "Prints this help information.", true ),
                       Functions::printCommands );
        functions.put( new WresFunction( "-i",
                                         "ingest",
                                         "Ingests data supplied in a declaration path or string. Example "
                                         + "usage: ingest /foo/bar/project.xml", false ),
                       Functions::ingest );
        functions.put( new WresFunction( "-m",
                                         "migrate",
                                         "Migrates a project declaration from XML (old-style) to YAML "
                                         + "(new style). Example usage: migrate /foo/bar/project_config.xml", true ),
                       Functions::migrate );
        functions.put( new WresFunction( "-md", "migratedatabase", "Migrate the WRES database.", false ),
                       Functions::migrateDatabase );
        functions.put( new WresFunction( "-mi",
                                         "migrateinline",
                                         "Migrates a project declaration from XML (old-style) to YAML "
                                         + "(new style). In addition, if the declaration references any external "
                                         + "sources of CSV thresholds, these will be migrated inline to the "
                                         + "declaration. Example usage: migrateinline /foo/bar/project_config.xml",
                                         true ),
                       Functions::migrateInline );
        functions.put( new WresFunction( "-r", "refreshdatabase", "Refreshes the database.", false ),
                       Functions::refreshDatabase );
        functions.put( new WresFunction( "-s", "server", "Spins up a long running worker server", true ),
                       Functions::startServer );
        functions.put( new WresFunction( "-v",
                                         "validate",
                                         "Validates the declaration supplied as a path or string. Example "
                                         + "usage: validate /foo/bar/project.xml", true ),
                       Functions::validate );
        functions.put( new WresFunction( "-vg",
                                         "validategrid",
                                         "Validates a netcdf grid supplied as a path and a corresponding variable "
                                         + "name. Example usage: validategrid /foo/bar/grid.nc baz", true ),
                       Functions::validateNetcdfGrid );

        return functions;
    }

    /**
     * Executes an evaluation.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    private static ExecutionResult execute( SharedResources sharedResources )
    {
        Evaluator evaluator = new Evaluator( sharedResources.systemSettings(),
                                             sharedResources.database(),
                                             sharedResources.brokerConnectionFactory() );

        List<String> args = sharedResources.arguments();
        if ( args.size() != 1 )
        {
            String message = "Please supply a (single) path to a project declaration file to evaluate, like this: "
                             + "bin/wres.bat execute c:/path/to/project_config.xml";
            LOGGER.error( message );
            UserInputException e = new UserInputException( message );
            return ExecutionResult.failure( e ); // Or return 400 - Bad Request (see #41467)
        }

        // Attempt to read the declaration
        String declarationOrPath = args.get( 0 )
                                       .trim();

        return evaluator.evaluate( declarationOrPath );
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
        if ( !sharedResources.systemSettings()
                             .isUseDatabase() )
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
            return ExecutionResult.failure( e );
        }
        finally
        {
            lockManager.shutdown();
        }

        return ExecutionResult.success();
    }

    /**
     * Creates the "migrateDatabase" method
     *
     * @return A method that will attempt to migrate the specified database according to steps in the db.changelog-master.xml
     */
    private static ExecutionResult migrateDatabase( SharedResources sharedResources )
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
            //The migrateDatabase method deals with database locking, so we don't need to worry about that here
            DatabaseOperations.migrateDatabase( sharedResources.database() );
        }
        catch ( SQLException se )
        {
            String message = "Failed to migrate the database.";
            LOGGER.error( message, se );
            InternalWresException e = new InternalWresException( message, se );
            return ExecutionResult.failure( e );
        }

        return ExecutionResult.success();
    }

    /**
     * Validates a NetCDF grid for ingest.
     * @param sharedResources the shared resources
     * @return the execution result
     */
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

    /**
     * Refreshes the core application database, where applicable.
     * @param sharedResources the shared resources
     * @return the execution result
     */

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

    /**
     * Starts a long-running worker server that takes in a port to run on.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    private static ExecutionResult startServer( final SharedResources sharedResources )
    {
        List<String> args = sharedResources.arguments();
        if ( args.size() != 1 )
        {
            String message = "Please supply a host for the server to listen to, like this: "
                             + "bin/wres.bat server 8010";
            LOGGER.error( message );
            UserInputException e = new UserInputException( message );
            return ExecutionResult.failure( e ); // Or return 400 - Bad Request (see #41467)
        }

        // Attempt to read the port
        String port = args.get( 0 ).trim();

        try
        {
            WebServer.main( new String[] { port } );
        }
        catch ( Exception e )
        {
            LOGGER.error( "Failed to start the server", e );
            return ExecutionResult.failure( e );
        }

        return ExecutionResult.success();
    }

    /**
     * Performs ingest of datasets into the core application database.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    private static ExecutionResult ingest( SharedResources sharedResources )
    {
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
                DatabaseCaches caches = DatabaseCaches.of( sharedResources.database() );
                TimeSeriesIngester timeSeriesIngester =
                        new DatabaseTimeSeriesIngester.Builder().setSystemSettings( sharedResources.systemSettings() )
                                                                .setDatabase( sharedResources.database() )
                                                                .setCaches( caches )
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
                                   griddedFeatures );

                lockManager.unlockShared( DatabaseType.SHARED_READ_OR_EXCLUSIVE_DESTROY_NAME );
                return ExecutionResult.success( declaration.label(), declarationString );
            }
            catch ( RuntimeException | SQLException e )
            {
                LOGGER.error( "Failed to ingest from {}", projectPath, e );
                return ExecutionResult.failure( declaration.label(), declarationString, e );
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

    /**
     * Validates the project declaration.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    private static ExecutionResult validate( SharedResources sharedResources )
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
                return ExecutionResult.success();
            }
            catch ( IOException | DeclarationException error )
            {
                LOGGER.warn( "The supplied declaration is invalid.", error );
                String message = "Failed to unmarshal and validate the project declaration at '"
                                 + pathOrDeclaration
                                 + "'";
                UserInputException e = new UserInputException( message, error );
                return ExecutionResult.failure( rawDeclaration, e ); // Or return 400 - Bad Request (see #41467)
            }
        }
        else
        {
            String message = "Could not find a project declaration to validate. Usage: validate <path to project>";
            LOGGER.error( message );
            UserInputException e = new UserInputException( message );
            return ExecutionResult.failure( e );
        }
    }

    /**
     * Migrates an old-style XML declaration to a new-style YAML declaration.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    private static ExecutionResult migrate( SharedResources sharedResources )
    {
        return Functions.migrate( sharedResources, false );
    }

    /**
     * Migrates an old-style XML declaration to a new-style YAML declaration. If the declaration includes any external
     * sources of CSV thresholds, they will be migrated inline to the declaration.
     * @param sharedResources the shared resources
     * @return the execution result
     */

    private static ExecutionResult migrateInline( SharedResources sharedResources )
    {
        return Functions.migrate( sharedResources, true );
    }

    /**
     * Migrates an old-style XML declaration to a new-style YAML declaration.
     * @param sharedResources the shared resources
     * @param inlineThresholds is true to migrate any external CSV thresholds inline to the declaration
     * @return the execution result
     */

    private static ExecutionResult migrate( SharedResources sharedResources, boolean inlineThresholds )
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

                return ExecutionResult.success();
            }
            catch ( UserInputException | IOException e )
            {
                LOGGER.error( "Failed to unmarshal project declaration from command line argument.", e );
                return ExecutionResult.failure( e );
            }
        }
        else
        {
            String message = "The declaration path or string was missing. Usage: validate "
                             + "<path to declaration or string>";
            UserInputException e = new UserInputException( message );
            return ExecutionResult.failure( e );
        }
    }

    /**
     * Creates a NetCDT template.
     * @param sharedResources the shared resources
     * @return the execution result
     */
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
                                                                + "that it is a Netcdf "
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
    record SharedResources( SystemSettings systemSettings,
                            Database database,
                            BrokerConnectionFactory brokerConnectionFactory,
                            String operation,
                            List<String> arguments )
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

            // If this is not a simple operation, it requires a broker connection factory
            if ( !arguments.isEmpty() && !Functions.isSimpleOperation( operation ) )
            {
                Objects.requireNonNull( brokerConnectionFactory );

                // If this complex operation involves a database, check that one exists
                if ( systemSettings.isUseDatabase() )
                {
                    Objects.requireNonNull( database );
                }
            }
        }
    }

    /**
     * Small wrapper for WRES functions.
     * @param shortName the short name
     * @param longName the long name
     * @param description the description
     * @param isSimpleOperation whether the operation is "simple" and does not, therefore, require spin-up or tear-down
     */
    private record WresFunction( String shortName,
                                 String longName,
                                 String description,
                                 boolean isSimpleOperation )
    {
        @Override
        public String toString()
        {
            String returnMe = "";

            if ( Objects.nonNull( this.shortName() ) )
            {
                returnMe = this.shortName();
                if ( Objects.isNull( this.longName() ) )
                {
                    returnMe = StringUtils.rightPad( returnMe, 30 );
                }
                else
                {
                    returnMe = returnMe + ", ";
                }
            }

            if ( Objects.nonNull( this.longName() ) )
            {
                returnMe = returnMe + this.longName();
                returnMe = StringUtils.rightPad( returnMe, 30 );
            }

            if ( Objects.nonNull( this.description() ) )
            {
                returnMe = returnMe + this.description();
            }

            return returnMe;
        }
    }

    /** Do not construct. */
    private Functions()
    {
    }
}
