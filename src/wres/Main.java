package wres;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import wres.events.broker.BrokerConnectionFactory;
import wres.events.broker.BrokerUtilities;
import wres.events.broker.CouldNotLoadBrokerConfigurationException;
import wres.eventsbroker.embedded.CouldNotStartEmbeddedBrokerException;
import wres.eventsbroker.embedded.EmbeddedBroker;
import wres.io.database.Database;
import wres.io.database.DatabaseOperations;
import wres.pipeline.InternalWresException;
import wres.pipeline.UserInputException;
import wres.system.SystemSettings;
import wres.util.Collections;

import com.google.common.collect.Range;

/**
 * Entry point for the standalone application.
 *
 * @author James Brown
 * @author Jesse Bickel
 * @author Chris Tubbs
 */
public class Main
{
    // Allow for logging of the native process ID of the process running this standalone
    static
    {
        ProcessHandle processHandle = ProcessHandle.current();
        long pid = processHandle.pid();
        MDC.put( "pid", Long.toString( pid ) );
    }

    private static final Logger LOGGER = LoggerFactory.getLogger( Main.class );
    private static final SystemSettings SYSTEM_SETTINGS = SystemSettings.fromDefaultClasspathXmlFile();
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
            LOGGER.info( Main.getVerboseRuntimeDescription() );
        }

        // Default to help function, -h
        String function = "-h";
        String[] finalArgs = args;

        // No arguments or request for help, print help without any other start-up details
        if ( args.length == 0 || ( args.length == 1 && ( "-h".equals( args[0] ) || "--help".equals( args[0] ) )
                                   || "help".equals( args[0] ) ) )
        {
            Functions.call( function, null );

            return;
        }
        // One argument that looks like a project declaration, so default to execute
        else if ( args.length == 1 && ( args[0].endsWith( ".xml" ) || args[0].startsWith( "<?xml " ) ) )
        {
            LOGGER.info( "Interpreting the first argument as project declaration and executing it..." );

            function = "execute";
        }
        // Apply the known function
        else if ( Functions.hasOperation( args[0] ) )
        {
            function = args[0];

            // Remove the function from the args
            finalArgs = Collections.removeIndexFromArray( args, 0 );
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

        LOGGER.info( "Running function '{}' with arguments '{}'.", finalFunction, finalArgs );

        Instant beganExecution = Instant.now();

        // Log any uncaught exceptions
        UncaughtExceptionHandler handler = ( a, b ) -> {
            String message = "Encountered an uncaught exception in thread " + a + ".";
            LOGGER.error( message, b );
        };

        Thread.setDefaultUncaughtExceptionHandler( handler );

        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            Instant endedExecution = Instant.now();
            Duration duration = Duration.between( beganExecution, endedExecution );
            LOGGER.info( "The function '{}' took {}", finalFunction, duration );
        } ) );

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

        try ( BrokerConnectionFactory brokerConnectionFactory =
                      BrokerConnectionFactory.of( brokerConnectionProperties ) )
        {
            Functions.SharedResources sharedResources =
                    new Functions.SharedResources( SYSTEM_SETTINGS,
                                                   database,
                                                   brokerConnectionFactory,
                                                   args );

            result = Functions.call( function, sharedResources );
            Instant endedExecution = Instant.now();
            String exception = null;

            if ( Objects.nonNull( result.getException() ) )
            {
                exception = ExceptionUtils.getStackTrace( result.getException() );
            }

            // Log the execution to the database if a database is used
            if ( SYSTEM_SETTINGS.isInDatabase() )
            {
                // Log both the operation and the args
                String[] argsToLog = new String[args.length + 1];
                argsToLog[0] = function;
                System.arraycopy( args, 0, argsToLog, 1, args.length );

                sharedResources.database()
                               .logExecution( argsToLog,
                                              result.getName(),
                                              result.getHash(),
                                              Range.open( beganExecution, endedExecution ),
                                              result.failed(),
                                              exception,
                                              Main.getVersion() );
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
        catch ( IOException e )
        {
            LOGGER.warn( "Failed to destroy the broker connections.", e );
        }
        finally
        {
            if ( SYSTEM_SETTINGS.isInDatabase() )
            {
                // #81660
                if ( Objects.nonNull( result ) && result.succeeded() )
                {
                    Functions.shutdown( database );
                }
                else
                {
                    Functions.forceShutdown( database, 6, TimeUnit.SECONDS );
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
                    LOGGER.warn( "Failed to destroy the embedded broker used for statistics messaging.", e );
                }
            }
        }

        // Exit
        Main.exit( result );
    }

    /**
     * Returns a database, if required, and migrates it as needed.
     *
     * @return a database or null 
     */

    private static Database getAndMigrateDatabaseIfRequired()
    {
        Database database = null;
        if ( SYSTEM_SETTINGS.isInDatabase() )
        {
            database = new Database( SYSTEM_SETTINGS );

            // Migrate the database, as needed
            if ( SYSTEM_SETTINGS.getDatabaseSettings()
                                .getAttemptToMigrate() )
            {
                try
                {
                    DatabaseOperations.migrateDatabase( database );
                }
                catch ( SQLException | IOException e )
                {
                    throw new IllegalStateException( "Failed to migrate the WRES database.", e );
                }
            }
        }

        return database;
    }

    /**
     * Exits the application. 
     *
     * @param result the execution result on exit
     */

    private static void exit( ExecutionResult result )
    {
        if ( Objects.nonNull( result ) && result.failed() )
        {
            if ( result.getException() instanceof UserInputException )
            {
                System.exit( 4 );
            }
            else if ( result.getException() instanceof InternalWresException )
            {
                System.exit( 5 );
            }
            else
            {
                System.exit( 1 );
            }
        }
    }

    public static String getVersion()
    {
        return version.toString();
    }

    public static String getVersionDescription()
    {
        return version.getDescription();
    }

    private static String getVerboseRuntimeDescription()
    {
        return version.getVerboseRuntimeDescription();
    }

}
