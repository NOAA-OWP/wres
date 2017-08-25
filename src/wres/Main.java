package wres;

import java.lang.management.ManagementFactory;
import java.time.OffsetDateTime;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            MainFunctions.shutdown();
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

        Integer exitCode = null;

        try
        {
            exitCode = MainFunctions.call( operation, cutArgs );
        }
        catch ( Exception e )
        {
            LOGGER.error( "Operation {} completed unsuccessfully", operation, e );
        }

        if (exitCode == null)
        {
            exitCode = MainFunctions.FAILURE;
        }

        watch.stop();

		System.exit(exitCode);
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
}
