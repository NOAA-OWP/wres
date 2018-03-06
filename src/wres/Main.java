package wres;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import wres.io.Operations;
import wres.util.Collections;
import wres.util.FormattedStopwatch;
import wres.util.Strings;
import wres.util.TimeHelper;

/**
 * @author Christopher Tubbs
 * Provides the entry point for prototyping development
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final Version version = new Version();

	/**
	 * Executes and times the requested operation with the given parameters
	 * @param args Arguments from the command line of the format {@code action <parameter 1, parameter 2, etc>}"
	 */
	public static void main(String[] args) {

		String processName = ManagementFactory.getRuntimeMXBean().getName();
		String processId = Strings.extractWord(processName, "\\d+(?=@)");

		MDC.put("pid", processId);

        if (LOGGER.isInfoEnabled())
        {
            LOGGER.info(getVersion());
        }

        final String operation = ((Supplier<String>) () -> {
            String op = "-h";
            if (args.length > 0 && MainFunctions.hasOperation(args[0])) {
                op = args[0];
            }
            else if (args.length > 0) {
                LOGGER.info(String.format("Running \"%s\" is not currently supported.", args[0]));
                LOGGER.info("Custom handling needs to be added to prototyping.Prototype.main ");
                LOGGER.info("to test the indicated prototype.");
            }
            return op;
        }).get();


        FormattedStopwatch watch = new FormattedStopwatch();

        final AtomicInteger exitCode = new AtomicInteger( MainFunctions.FAILURE );

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if ( exitCode.get() == MainFunctions.SUCCESS )
            {
                MainFunctions.shutdown();
            }
            else
            {
                MainFunctions.forceShutdown( 6, TimeUnit.SECONDS );
            }
            LOGGER.info("The function '{}' took {}", operation, watch.getFormattedDuration());
        }));

        String[] cutArgs = Collections.removeIndexFromArray(args, 0);
        String process = "Process: ";
        process += processId;
        LOGGER.info(process);

        LOGGER.info( "Beginning operation: '" +
                     operation +
                     "' at " +
                     TimeHelper.convertDateToString( OffsetDateTime.now()) +
                     "...");
        watch.start();

        // The following two are for logging run information to the database.
        long startTime = System.currentTimeMillis();
        Long duration;

        try
        {
            exitCode.set( MainFunctions.call( operation, cutArgs ) );

            if (exitCode.get() != MainFunctions.SUCCESS)
            {
                throw new Exception( "An operation failed. Please consult the "
                                     + "logs for more details." );
            }

            duration = System.currentTimeMillis() - startTime;

            Operations.logExecution( args,
                                     startTime,
                                     duration,
                                     exitCode.get() == MainFunctions.FAILURE,
                                     Main.combineExceptions() );
        }
        catch ( Exception e )
        {
            duration = System.currentTimeMillis() - startTime;
            String message = "Operation '" + operation + "' completed unsuccessfully";
            LOGGER.error( message, e );
            Operations.logExecution( args,
                                     startTime,
                                     duration,
                                     exitCode.get() == MainFunctions.FAILURE,
                                     Main.combineExceptions( ) );
        }
        watch.stop();

        System.out.println( "Log messages have been written to the file "
                            + System.getProperty("user.home")
                            + "/wres_logs/wres.log (unless otherwise configured"
                            + " in lib/conf/logback.xml)." );

        System.exit( exitCode.get() );
	}

    public static String getVersion()
    {
        return version.toString();
    }

    /**
     * Combines errors occurred throughout all highlevel processing
     * @return
     */
    private static String combineExceptions()
    {
        List<Exception> encounteredExceptions = MainFunctions.getEncounteredExceptions();
        List<String> messages = new ArrayList<>();
        String message = "";

        String separator = System.lineSeparator() +
                           "------------------------------------------------------------------------" +
                           System.lineSeparator();

        if (encounteredExceptions.size() > 0)
        {
            for ( Exception exception : encounteredExceptions )
            {
                messages.add( Strings.getStackTrace( exception ) );
            }

            message = String.join( separator, messages );
        }

        return message;
    }
}
