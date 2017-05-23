/**
 * 
 */

import java.sql.SQLException;

import util.MainFunctions;
import util.Stopwatch;
import util.Utilities;

/**
 * @author Christopher Tubbs
 * Provides the entry point for prototyping development
 */
public class Main {
	
	/**
	 * Executes and times the requested operation with the given parameters
	 * @param args Arguments from the command line of the format {@code action <parameter 1, parameter 2, etc>}"
	 */
	public static void main(String[] args) {

		if (args.length > 0)
		{
			String operation = args[0];
			
			if (MainFunctions.hasOperation(operation))
			{
			    try
                {
                    Utilities.initializeCaches();
                }
                catch(SQLException e1)
                {
                    System.err.println("The cache could not be initialized.");
                    e1.printStackTrace();
                }
				args = Utilities.removeIndexFromArray(args, 0);
				System.out.println("Beginning operation: '" + operation + "'...");

                Stopwatch watch = new Stopwatch();
                watch.start();
				try
				{
					MainFunctions.call(operation, args);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				
				watch.stop();
				
				System.out.print("The function '");
				System.out.print(operation);
				System.out.print("' took ");
				System.out.println(watch.getFormattedDuration());

				System.out.println();
				System.out.println(Utilities.getSystemStats());
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
