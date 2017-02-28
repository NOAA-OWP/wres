package wres;
import wres.util.MainFunctions;

/**
 * 
 */

/**
 * @author ctubbs
 *
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length > 0)
		{
			String operation = args[0];
			
			if (MainFunctions.has_operation(operation))
			{
				args = wres.util.Utilities.removeIndexFromArray(args, String.class, 0);
				System.out.println("Beginning operation: '" + operation + "'...");
				long start_time = System.nanoTime();
				try
				{
					MainFunctions.call(operation, args);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				float end_time = System.nanoTime();
				float duration = (end_time - start_time) / 1000000.0f;
				String time_unit = " milliseconds";
				
				if (duration > 60000)
				{
					duration = duration / 60000;
					time_unit = " minutes";
				}
				else if (duration > 1000)
				{
					duration = duration / 1000;
					time_unit = " seconds";
				}
				
				System.out.print("The function '");
				System.out.print(operation);
				System.out.print("' took ");
				System.out.print(String.valueOf(duration));
				System.out.println(time_unit);
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
		

	}

}
