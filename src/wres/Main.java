package wres;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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
import wres.util.Time;

/**
 * @author Christopher Tubbs
 * Provides the entry point for prototyping development
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

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
                MainFunctions.shutdownWithAbandon( 6, TimeUnit.SECONDS );
            }
            LOGGER.info("The function '{}' took {}", operation, watch.getFormattedDuration());
        }));

        String[] cutArgs = Collections.removeIndexFromArray(args, 0);
        String process = "Process: ";
        process += processId;
        LOGGER.info(process);

        LOGGER.info("Beginning operation: '" +
                                   operation +
                                   "' at " +
                                   Time.convertDateToString(OffsetDateTime.now()) +
                                   "...");
        watch.start();

        // The following two are for logging run information to the database.
        long startTime = System.currentTimeMillis();
        long endTime;

        try
        {
            exitCode.set( MainFunctions.call( operation, cutArgs ) );
        }
        catch ( Exception e )
        {
            endTime = System.currentTimeMillis();
            LOGGER.error( "Operation {} completed unsuccessfully", operation, e );

            Operations.logExecution( args,
                                     Main.sqlDateFromMillis( startTime ),
                                     Main.sqlDateFromMillis( endTime ),
                                     true );
        }

        endTime = System.currentTimeMillis();
        watch.stop();

        Operations.logExecution( args,
                                 Main.sqlDateFromMillis( startTime ),
                                 Main.sqlDateFromMillis( endTime ),
                                 exitCode.get() != MainFunctions.SUCCESS );

        System.out.println( "Log messages have been written to the file "
                            + System.getProperty("user.home")
                            + "/wres_logs/wres.log (unless otherwise configured"
                            + " in lib/conf/logback.xml)." );

        System.exit( exitCode.get() );
	}

    public static String getVersion()
    {
        // Empty object for only getting version of the software
        Package toGetVersion = (new MainFunctions()).getClass().getPackage();

        if (toGetVersion != null && toGetVersion.getImplementationVersion() != null)
        {
            // When running from a released zip, the version should show up.
            return "WRES version " + toGetVersion.getImplementationVersion();
        }
        else
        {
            // When running from source, this will be the expected outcome.
            return "WRES version is unknown, probably developer version.";
        }
    }


    private static String sqlDateFromMillis( long millis )
    {
        final String PATTERN = "YYYY-MM-dd HH:mm:ss.SSSZ";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern( PATTERN,
                                                                   Locale.US )
                                                       .withZone( ZoneId.systemDefault() );
        Instant instant = Instant.ofEpochMilli( millis );
        return formatter.format( instant );
    }

}
