package wres;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import wres.config.MultiDeclarationFactory;
import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.broker.CouldNotLoadBrokerConfigurationException;
import wres.eventsbroker.embedded.CouldNotStartEmbeddedBrokerException;
import wres.eventsbroker.embedded.EmbeddedBroker;
import wres.io.database.ConnectionSupplier;
import wres.io.database.Database;
import wres.io.database.DatabaseOperations;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.system.SettingsFactory;
import wres.system.SystemSettings;

/**
 * Entry point for the standalone application.
 *
 * @author James Brown
 * @author Jesse Bickel
 * @author Chris Tubbs
 */
public class Main
{
    // Allow for logging of the native ID of the process running this standalone
    static
    {
        ProcessHandle processHandle = ProcessHandle.current();
        long pid = processHandle.pid();
        MDC.put( "pid", Long.toString( pid ) );
    }

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Main.class );
    /** System settings. */
    private static final SystemSettings SYSTEM_SETTINGS = SettingsFactory.createSettingsFromDefaultXml();
    /** Software version information. */
    private static final Version version = new Version( SYSTEM_SETTINGS );

    /**
     * Executes and times the requested operation with the given parameters
     * @param args Arguments from the command line of the format {@code action <parameter 1, parameter 2, etc>}"
     */
    public static void main( String[] args )
    {
        // Print some information about the software version and runtime
        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( Main.getVersionDescription() );
            LOGGER.info( Main.getVerboseRuntimeDescription( SYSTEM_SETTINGS ) );
        }

        // Default to help function, -h
        String function = "-h";
        String[] finalArgs = args;

        // Is this a simple operation that does not require any stand-up or tear-down, such as "help"?
        if ( Main.isSimpleOperation( args ) )
        {
            Main.runSimpleOperation( args );
            return;
        }
        // One argument that looks like a project declaration, so default to execute
        else if ( args.length == 1 && MultiDeclarationFactory.isDeclarationPathOrString( args[0] ) )
        {
            LOGGER.info( "Interpreting the first argument as project declaration and executing it..." );

            function = "execute";
        }
        // Apply the known function
        else if ( Functions.hasOperation( args[0] ) )
        {
            function = args[0];

            // Remove the function from the args
            finalArgs = Main.removeOperationFromArgs( args );
        }
        // Unknown function, log and print help information without any other start-up details
        else
        {
            LOGGER.warn( "The function \"{}\" is not supported.", args[0] );

            // Print help information
            Functions.call( function, null );

            return;
        }

        final String finalFunction = function;

        // Report the function and arguments if possible
        if ( LOGGER.isInfoEnabled() )
        {
            List<String> curtailedArgs = Arrays.stream( finalArgs )
                                               .map( Functions::curtail )
                                               .toList();
            LOGGER.info( "Running function '{}' with arguments '{}'.", finalFunction, curtailedArgs );
        }

        Instant beganExecution = Instant.now();

        // Log any uncaught exceptions
        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an uncaught exception in thread " + a + ".";
            LOGGER.error( message, b );
        };

        Thread.setDefaultUncaughtExceptionHandler( handler );

        String pid = MDC.get( "pid" );

        if ( Objects.nonNull( pid ) )
        {
            String process = "Process: ";
            process += MDC.get( "pid" );
            LOGGER.info( process );
        }
        else
        {
            LOGGER.warn( "Failed to find the process id" );
        }

        Main.completeExecution( finalFunction, finalArgs, beganExecution );
    }

    /**
     * Determines whether the function requires any stand-up or tear-down or involves a simple execution. For example,
     * a request for "help" is a simple execution.
     *
     * @return whether the command line arguments contain a simple operation
     */

    private static boolean isSimpleOperation( String[] args )
    {
        return args.length == 0 || Functions.isSimpleOperation( args[0] );
    }

    /**
     * Runs a simple operation that does not require any stand-up or tear-down.
     * @param args the arguments
     */
    private static void runSimpleOperation( String[] args )
    {
        // Default to help
        String function = "-h";
        if ( args.length > 0 )
        {
            function = args[0];
        }

        LOGGER.debug( "Discovered a simple operation to execute: {}.", function );

        // Remove the operation from the arguments and list them
        String[] finalArgs = Main.removeOperationFromArgs( args );
        List<String> argList = Arrays.stream( finalArgs )
                                     .toList();

        Functions.SharedResources sharedResources =
                new Functions.SharedResources( SYSTEM_SETTINGS,
                                               null,
                                               null,
                                               function,
                                               argList );

        Functions.call( function, sharedResources );
    }

    /**
     * Removes the operation in the first index of the argument array from the array
     * @param args the arguments whose first argument should be removed
     * @return the adjusted arguments
     */
    private static String[] removeOperationFromArgs( String[] args )
    {
        if ( args.length < 2 )
        {
            return args;
        }

        // Remove the function from the args
        String[] finalArgs = new String[args.length - 1];
        System.arraycopy( args, 1, finalArgs, 0, finalArgs.length );
        return finalArgs;
    }

    /**
     * Completes an execution.
     * @param function the function to execute
     * @param args the arguments to execute
     * @param beganExecution the time when the execution began
     */

    private static void completeExecution( String function, String[] args, Instant beganExecution )
    {
        ExecutionResult result = null;
        Database database = Main.getAndMigrateDatabaseIfRequired();

        // Create the broker connections for statistics messaging
        Properties brokerConnectionProperties =
                BrokerUtilities.getBrokerConnectionProperties( BrokerConnectionFactory.DEFAULT_PROPERTIES );

        // Create an embedded broker for statistics messages, if needed
        EmbeddedBroker broker = null;
        if ( BrokerUtilities.isEmbeddedBrokerRequired( brokerConnectionProperties ) )
        {
            broker = EmbeddedBroker.of( brokerConnectionProperties, false );
        }

        // Create the broker connection factory
        BrokerConnectionFactory brokerConnectionFactory = BrokerConnectionFactory.of( brokerConnectionProperties );

        try
        {
            List<String> argList = Arrays.stream( args )
                                         .toList();
            Functions.SharedResources sharedResources =
                    new Functions.SharedResources( SYSTEM_SETTINGS,
                                                   database,
                                                   brokerConnectionFactory,
                                                   function,
                                                   argList );

            result = Functions.call( function, sharedResources );
            Instant endedExecution = Instant.now();

            // Log the execution to the database if a database is used
            if ( SYSTEM_SETTINGS.isUseDatabase() )
            {
                // Log both the operation and the args
                String[] argsToLog = new String[args.length + 1];
                argsToLog[0] = function;
                System.arraycopy( args, 0, argsToLog, 1, args.length );

                DatabaseOperations.LogParameters logParameters =
                        new DatabaseOperations.LogParameters( Arrays.stream( argsToLog )
                                                                    .toList(),
                                                              result.getName(),
                                                              result.getDeclaration(),
                                                              result.getHash(),
                                                              beganExecution,
                                                              endedExecution,
                                                              result.failed(),
                                                              result.getException(),
                                                              Main.getVersion() );

                DatabaseOperations.logExecution( database,
                                                 logParameters );
            }

            if ( result.failed() )
            {
                String message = "Operation '" + function
                                 + "' completed unsuccessfully";
                LOGGER.error( message, result.getException() );
            }
        }
        catch ( CouldNotLoadBrokerConfigurationException | CouldNotStartEmbeddedBrokerException e )
        {
            LOGGER.warn( "Failed to create the broker connections.", e );
        }
        finally
        {
            LOGGER.info( "Closing the application..." );

            if ( SYSTEM_SETTINGS.isUseDatabase() && Objects.nonNull( database ) )
            {
                // #81660
                if ( Objects.nonNull( result ) && result.succeeded() )
                {
                    LOGGER.info( "Closing database activities..." );
                    database.shutdown();
                    LOGGER.info( "The database activities have been closed." );
                }
                else
                {
                    LOGGER.info( "Forcefully closing database activities (you may see some errors)..." );
                    List<Runnable> abandoned = database.forceShutdown( 6, TimeUnit.SECONDS );
                    LOGGER.info( "Abandoned {} database tasks.", abandoned.size() );
                }
            }

            if ( Objects.nonNull( broker ) )
            {
                try
                {
                    broker.close();
                }
                catch ( IOException e )
                {
                    LOGGER.warn( "Failed to close the embedded broker.", e );
                }
            }

            LOGGER.info( "The application has been closed." );
        }

        // Exit non-zero, if required. A nominal exit should not reach this point as clean-up has completed and all
        // remaining threads should be daemon threads, allowing the JVM to exit cleanly. This is a last resort.
        Main.forceExitOnErrorIfNeeded( result );
    }

    /**
     * Returns a database, if required, and migrates it as needed.
     *
     * @return a database or null 
     */

    private static Database getAndMigrateDatabaseIfRequired()
    {
        Database database = null;
        if ( SYSTEM_SETTINGS.isUseDatabase() )
        {
            database = new Database( new ConnectionSupplier( SYSTEM_SETTINGS ) );

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

        return database;
    }

    /**
     * Exits the application with a non-zero exit status when an error is encountered and the application has not yet
     * exited.
     *
     * @param result the execution result on exit
     */

    private static void forceExitOnErrorIfNeeded( ExecutionResult result )
    {
        if ( Objects.nonNull( result )
             && result.failed() )
        {
            if ( result.getException() instanceof UserInputException )
            {
                LOGGER.warn( "Terminating with a non-zero exit code: 4 (user input error)." );
                System.exit( 4 );
            }
            else if ( result.getException() instanceof InternalWresException )
            {
                LOGGER.warn( "Terminating with a non-zero exit code: 5 (evaluation error)." );
                System.exit( 5 );
            }
            else
            {
                LOGGER.warn( "Terminating with a non-zero exit code: 1 (unknown error)." );
                System.exit( 1 );
            }
        }
    }

    /**
     * @return the software version
     */

    public static String getVersion()
    {
        return version.toString();
    }

    /**
     * @return the software version description
     */

    public static String getVersionDescription()
    {
        return version.getDescription();
    }

    /**
     * @param systemSettings The SystemSettings to print information on
     * @return a verbose runtime description
     */

    public static String getVerboseRuntimeDescription( SystemSettings systemSettings )
    {
        return version.getVerboseRuntimeDescription( systemSettings );
    }
}
