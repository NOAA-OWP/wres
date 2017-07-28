import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import util.MainFunctions;
import wres.util.Collections;
import wres.util.FormattedStopwatch;
import wres.util.Strings;
import wres.util.Time;

import java.lang.management.ManagementFactory;
import java.time.OffsetDateTime;

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

		Integer exitCode = -1;

		if (args.length > 0)
		{
			String operation = args[0];
			
			if (MainFunctions.hasOperation(operation))
			{
				Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    MainFunctions.shutdown();
                }));

				args = Collections.removeIndexFromArray(args, 0);
				String process = "Process: ";
				process += processId;
				System.out.println(process);

				System.out.println("Beginning operation: '" +
										   operation +
										   "' at " +
										   Time.convertDateToString(OffsetDateTime.now()) +
										   "...");

                FormattedStopwatch watch = new FormattedStopwatch();
                watch.start();

				Integer result = MainFunctions.call(operation, args);

				if (result == null)
				{
					result = MainFunctions.FAILURE;
				}

				exitCode = result;

				watch.stop();
				
				System.out.print("The function '");
				System.out.print(operation);
				System.out.print("' took ");
				System.out.println(watch.getFormattedDuration());

				System.out.println();
				System.out.println(Strings.getSystemStats());
				//MainFunctions.shutdown();
			}
			else
			{
				System.out.println(String.format("Running \"%s\" is not currently supported.", operation));
				System.out.print("Custom handling needs to be added to prototyping.Prototype.main ");
				System.out.println("to test the indicated prototype.");
			}
		}
		else
		{
			System.out.println("No prototype function has been specified");
		}

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
