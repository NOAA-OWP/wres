package wres;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import wres.config.DeclarationFactory;
import wres.events.broker.CouldNotLoadBrokerConfigurationException;
import wres.eventsbroker.embedded.CouldNotStartEmbeddedBrokerException;
import wres.helpers.MainUtilities;
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
    private static final Version version = new Version();

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
        else if ( args.length == 1
                  && DeclarationFactory.isValidDeclarationString( args[0], FileSystems.getDefault() ) )
        {
            LOGGER.info( "Interpreting the first argument as project declaration and executing it..." );

            function = "execute";
        }
        // Apply the known function
        else if ( MainUtilities.hasOperation( args[0] ) )
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
            MainUtilities.call( function, null );

            return;
        }

        final String finalFunction = function;

        // Report the function and arguments if possible
        if ( LOGGER.isInfoEnabled() )
        {
            List<String> curtailedArgs = Arrays.stream( finalArgs )
                                               .map( MainUtilities::curtail )
                                               .toList();
            LOGGER.info( "Running function '{}' with arguments '{}'.", finalFunction, curtailedArgs );
        }

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

        Main.completeExecution( finalFunction, finalArgs );
    }

    /**
     * Determines whether the function requires any stand-up or tear-down or involves a simple execution. For example,
     * a request for "help" is a simple execution.
     *
     * @return whether the command line arguments contain a simple operation
     */

    private static boolean isSimpleOperation( String[] args )
    {
        return args.length == 0 || MainUtilities.isSimpleOperation( args[0] );
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
                                               function,
                                               argList );

        MainUtilities.call( function, sharedResources );
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
     */

    private static void completeExecution( String function, String[] args )
    {
        ExecutionResult result = null;

        try
        {
            List<String> argList = Arrays.stream( args )
                                         .toList();
            Functions.SharedResources sharedResources =
                    new Functions.SharedResources( SYSTEM_SETTINGS,
                                                   function,
                                                   argList );

            result = MainUtilities.call( function, sharedResources );

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


        // Exit non-zero, if required. A nominal exit should not reach this point as clean-up has completed and all
        // remaining threads should be daemon threads, allowing the JVM to exit cleanly. This is a last resort.
        Main.forceExitOnErrorIfNeeded( result );
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
