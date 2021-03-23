package wres;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import wres.control.InternalWresException;
import wres.control.UserInputException;
import wres.io.concurrency.Executor;
import wres.io.utilities.Database;
import wres.system.SystemSettings;
import wres.util.Collections;

/**
 * @author Christopher Tubbs
 * Provides the entry point for prototyping development
 */
public class Main {
    static
    {
        ProcessHandle processHandle = ProcessHandle.current();
        long pid = processHandle.pid();
        MDC.put("pid", Long.toString( pid ) );
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final Version version = new Version();

	/**
	 * Executes and times the requested operation with the given parameters
	 * @param args Arguments from the command line of the format {@code action <parameter 1, parameter 2, etc>}"
	 */
	public static void main(String[] args) {


        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info( Main.getVersionDescription() );
            LOGGER.info( Main.getVerboseRuntimeDescription() );
        }

        String operation = "-h";

        if (args.length > 0 && MainFunctions.hasOperation(args[0]))
        {
            operation = args[0];
        }
        else if (args.length > 0)
        {
            LOGGER.info("Running \"{}\" is not currently supported.", args[0]);
            LOGGER.info("Custom handling needs to be added to prototyping.Prototype.main ");
            LOGGER.info("to test the indicated prototype.");
        }

        final String finalOperation = operation;
        Instant beganExecution = Instant.now();

        SystemSettings systemSettings = SystemSettings.fromDefaultClasspathXmlFile();
        Database database = new Database( systemSettings );
        Executor executor = new Executor( systemSettings );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Instant endedExecution = Instant.now();
            Duration duration = Duration.between( beganExecution, endedExecution );
            LOGGER.info( "The function '{}' took {}", finalOperation, duration );
        }));

        String[] cutArgs = Collections.removeIndexFromArray(args, 0);
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

        MainFunctions.SharedResources sharedResources =
                new MainFunctions.SharedResources( systemSettings,
                                                   database,
                                                   executor,
                                                   cutArgs );

        ExecutionResult result = null;

        try
        {
            result = MainFunctions.call( operation, sharedResources );
            Instant endedExecution = Instant.now();
            String exception = null;

            if ( Objects.nonNull( result.getException() ) )
            {
                exception = ExceptionUtils.getStackTrace( result.getException() );
            }

            sharedResources.getDatabase()
                           .logExecution(
                                     args,
                                     result.getName(),
                                     beganExecution,
                                     endedExecution,
                                     result.failed(),
                                     exception,
                                     Main.getVersion() );

            if ( result.failed() )
            {
                String message = "Operation '" + operation
                                 + "' completed unsuccessfully";
                LOGGER.error( message, result.getException() );
            }
        }
        finally
        {
            // #81660
            if ( Objects.nonNull( result ) && result.succeeded() )
            {
                MainFunctions.shutdown( database, executor );
            }
            else
            {
                MainFunctions.forceShutdown( database, executor,6, TimeUnit.SECONDS );
            }
        }

        Main.printLogFileInformation();

        if ( result.failed() )
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


    /**
     * Print some hints to stdout about log files.
     *
     * This violates the rule of avoiding standard out and standard error
     * because if you are already looking at the log output, you do not need
     * this extraneous information.
     *
     * Printing log file information is dubious since the system administrator
     * has control at runtime over which log file to use and can set debug on
     * the runtime logging facilities chosen, but this is a convenience that has
     * been requested in #37382 and #51945. The logging library is chosen
     * separately from the application, so attempting to print from inside the
     * application at compile-time violates SOC (slf4j at compile-time).
     *
     * The logging library chosen should allow the admin to print information
     * about which file it is logging to. For example, with logback, one can
     * set debug="true" in the configuration tag to show additional information.
     */

    private static void printLogFileInformation()
    {
        String logFileOverride = System.getProperty( "logback.configurationFile" );

        String messagesWritten = "Log messages have been written to the file ";

        if ( logFileOverride != null && !logFileOverride.isEmpty() )
        {
            System.out.println( messagesWritten +
                                "specified in the logback configuration file "
                                + logFileOverride + ". For more details, use "
                                + "the logging library's debug functionality." );
        }
        else
        {
            System.out.println( messagesWritten
                                + System.getProperty( "user.home" )
                                + "/wres_logs/wres.log (unless otherwise configured"
                                + " in lib/conf/logback.xml)." );
        }
    }
}
