package wres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import util.MainFunctions;
import wres.io.Operations;
import wres.util.Collections;
import wres.util.FormattedStopwatch;
import wres.util.Strings;
import wres.util.Time;

import java.lang.management.ManagementFactory;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
            if (args.length > 0) {
                op = args[0];
            }
            else if (!MainFunctions.hasOperation(args[0])) {
                System.out.println(String.format("Running \"%s\" is not currently supported.", args[0]));
                System.out.print("Custom handling needs to be added to prototyping.Prototype.main ");
                System.out.println("to test the indicated prototype.");
            }
            return op;
        }).get();

        FormattedStopwatch watch = new FormattedStopwatch();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            MainFunctions.shutdown();

            System.out.print("The function '");
            System.out.print(operation);
            System.out.print("' took ");
            System.out.println(watch.getFormattedDuration());
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

        Integer exitCode = MainFunctions.call(operation, cutArgs);

        if (exitCode == null)
        {
            exitCode = MainFunctions.FAILURE;
        }

        watch.stop();

        String arguments = Arrays.asList(args)
                                 .stream()
                                 .map(Object::toString)
                                 .collect(Collectors.joining(" "));

        Operations.logExecution(arguments,
                                MainFunctions.getRawProject(),
                                watch.getStartTime(),
                                watch.getStopTime(),
                                exitCode != MainFunctions.SUCCESS);

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
