/**
 * 
 */
package util;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import concurrency.Executor;
import concurrency.ForecastSaver;
import java.util.function.Consumer;
import reading.BasicSource;
import reading.SourceReader;
import config.ProjectConfig;
import config.SystemConfig;
import config.data.Project;

import java.util.concurrent.Future;
import concurrency.FunctionRunner;
import concurrency.Metrics;
import concurrency.ObservationSaver;
import data.Variable;;
/**
 * @author ctubbs
 *
 */
public final class MainFunctions {

	// Mapping of String names to corresponding methods
	private static final Map<String, Consumer<String[]>> functions = createMap();
	
	/**
	 * Determines if there is a method for the requested operation
	 * @param operation The desired operation to perform
	 * @return True if there is a method mapped to the operation name
	 */
	public static final boolean has_operation(String operation)
	{
		return functions.containsKey(operation.toLowerCase());
	}
	
	/**
	 * Executes the operation with the given list of arguments
	 * 
	 * @param operation The name of the desired method to call
	 * @param args The desired arguments to use when calling the method
	 */
	public static final void call(String operation, String[] args)
	{
		operation = operation.toLowerCase();
		functions.get(operation).accept(args);
		//Database.commit();	
		Executor.complete();
	}
	
	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
	private static final Map<String, Consumer<String[]>> createMap()
	{
		Map<String, Consumer<String[]>> prototypes = new HashMap<String, Consumer<String[]>>();
		
		prototypes.put("describenetcdf", describeNetCDF());
		prototypes.put("connecttodb", connectToDB());
		prototypes.put("saveforecast", saveForecast());
		prototypes.put("saveobservation", saveObservation());
		prototypes.put("getpairs", getPairs());
		prototypes.put("querynetcdf", queryNetCDF());
		prototypes.put("commands", print_commands());
		prototypes.put("--help", print_commands());
		prototypes.put("-h", print_commands());
		prototypes.put("meanerror", meanError());
		prototypes.put("systemmetrics", systemMetrics());
		prototypes.put("saveobservations", saveObservations());
		prototypes.put("saveforecasts", saveForecasts());
		prototypes.put("describeprojects", describeProjects());
		
		return prototypes;
	}
	
	/**
	 * Creates the "print_commands" method
	 * @return Method that prints all available commands by name
	 */
	private static final Consumer<String[]> print_commands()
	{
		return (String[] args) -> {
			System.out.println("Available commands are:");
			for (String command : functions.keySet())
			{
				System.out.println("\t" + command);
			}
		};
	}
	
	/**
	 * Creates the "saveForecast" method
	 * @return Method that will attempt to save the file at the given path to the database as forecasts
	 */
	private static final Consumer<String[]> saveForecast()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				try
				{
					BasicSource source = SourceReader.get_source(args[0]);
					System.out.println(String.format("Attempting to save '%s' to the database...", args[0]));
					source.save_forecast();
					System.out.println("Database save operation completed. Please verify data.");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to save data. Please pass that in as the first argument.");
				System.out.println("For now, ensure that the path points towards a tabular ASCII file.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
	
	/**
	 * Creates the "saveForecasts" method
	 * @return Method that will attempt to save all files in the given directory to the database as forecasts
	 */
	private static final Consumer<String[]> saveForecasts()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				try
				{
					String directory = args[0];
					File[] files = new File(directory).listFiles();
					System.out.println(String.format("Attempting to save all files in '%s' as forecasts to the database...", args[0]));
					for (File file : files)
					{
						Executor.execute(new ForecastSaver(file.getAbsolutePath()));
					}
					Executor.complete();
					System.out.println("All forecast saving operations complete. Please verify data.");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path to a directory is needed to save data. Please pass that in as the first argument.");
				System.out.println("usage: saveForecasts <directory path>");
			}
		};
	}
	
	/**
	 * Creates the "saveObservation" method
	 * @return Method that will attempt to save a file to the database as an observation
	 */
	private static final Consumer<String[]> saveObservation()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				try
				{
					BasicSource source = SourceReader.get_source(args[0]);
					System.out.println(String.format("Attempting to save '%s' to the database...", args[0]));
					source.save_observation();
					System.out.println("Database save operation completed. Please verify data.");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to save data. Please pass that in as the first argument.");
				System.out.println("For now, ensure that the path points towards a datacard file.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
	
	/**
	 * Creates a Method that will attempt to save all files in a directory as observations
	 * @return Method that will attempt to save all files in a directory to the database as observations
	 */
	private static final Consumer<String[]> saveObservations() 
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				try
				{
					String directory = args[0];
					File[] files = new File(directory).listFiles();
					System.out.println(String.format("Attempting to save all files in '%s' as observations to the database...", args[0]));
					for (File file : files)
					{
						Executor.execute(new ObservationSaver(file.getAbsolutePath()));
					}
					Executor.complete();
					System.out.println("All observation saving operations complete. Please verify data.");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path to a directory is needed to save data. Please pass that in as the first argument.");
				System.out.println("usage: saveObservations <directory path>");
			}
		};
	}
	
	/**
	 * Creates the "connectToDB" method
	 * @return method that will attempt to connect to the database to prove that a connection is possible. The version of the connected database will be printed.
	 */
	private static final Consumer<String[]> connectToDB()
	{
		return (String[] args) -> {
			try {
				String version = Database.get_result("Select version() AS version_detail", "version_detail"); 
				System.out.println(version);
				System.out.println("Successfully connected to the database");
			} catch (SQLException e) {
				System.out.println("Could not connect to database because:");
				e.printStackTrace();
			}
		};
	}
	
	@Deprecated
	/**
	 * Creates the "getPairs" method
	 * @deprecated The query involved retrieves data from an outdated schema. Considering that pairing operations are more complicated now,
	 * pairing definitions will need to be defined within a separate project file.
	 * @return A method that will gather and pair every value for a given variable
	 */
	private static final Consumer<String[]> getPairs()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				String variable = args[0];
				Connection connection = null;
				String script = "SELECT F.forecast_date,\n"	
								+ "		lead_time,\n"
								+ "		R.measurement,\n"
								+ "		FR.measurements\n"
								+ "FROM Forecast F\n"
								+ "INNER JOIN ForecastResult FR\n"
								+ "		ON FR.forecast_id = F.forecast_id\n"
								+ "INNER JOIN Observation O\n"
								+ "		ON O.variable_id = F.variable_id\n"
								+ "INNER JOIN ObservationResult R\n"
								+ "		ON R.observation_id = O.observation_id\n"
								+ "			AND R.valid_date = F.forecast_date + INTERVAL '1 hour' * FR.lead_time\n"
								+ "INNER JOIN Variable V\n"
								+ "		ON V.variable_id = F.variable_id\n"
								+ "WHERE V.variable_name = '" + variable + "'\n";
				
				if (args.length > 1)
				{
					Path path = Paths.get(args[1]);
					script += "		AND F.source = '" + path.toAbsolutePath().toString() + "'\n";
				}
				
				script += "ORDER BY forecast_date, FR.lead_time\n"
						+ "LIMIT 100;";
				
				try {
					connection = Database.get_connection();
					Statement query = connection.createStatement();
					ResultSet results = query.executeQuery(script);
					query.setFetchSize(SystemConfig.instance().get_fetch_size());
					System.out.println("Pair data is now in memory!");
					
					while (results.next())
					{
						System.out.print("\t\t");
						System.out.print(results.getInt("lead_time"));
						System.out.print(" |\t\t");
						System.out.print(results.getFloat("measurement"));
						System.out.print(" |\t");
						System.out.println(Utilities.toString((Float[])results.getArray("measurements").getArray()));
						System.out.println();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				finally
				{
					if (connection != null)
					{
						try {
							Database.return_connection(connection);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			else
			{
				System.out.println("Not enough arguments were passed.");
				System.out.println("*.jar getPairs <variable name> [<source name>]");
			}
		};
	}
	
	@Deprecated
	/**
	 * Creates the "meanError" method
	 * @deprecated Relies on an outdated schema. Like the pairing method above, details will need to be defined within an external
	 * configuration file.
	 * @return A method that will pair and determine the mean error for all matching data for the passed in variable name
	 */
	private static final Consumer<String[]> meanError()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				String variable = args[0];
				String start_date = "'1/1/1800'";
				String end_date = "'12/1/2400'";
				int lead = 0;
				String lead_script = "";
				Connection connection = null;
				
				TreeMap<Integer, Future<Double>> computed_errors = new TreeMap<Integer, Future<Double>>();
				TreeMap<Integer, Double> errors = new TreeMap<Integer, Double>();
				
				String script = "SELECT FR.lead_time\n"
								+ "FROM Forecast F\n"
								+ "INNER JOIN ForecastResult FR\n"
								+ "		ON FR.forecast_id = F.forecast_id\n"
								+ "INNER JOIN Variable V\n"
								+ "		ON V.variable_id = F.variable_id\n"
								+ "WHERE V.variable_name = '" + variable + "'\n"
								+ "		AND F.forecast_date >= " + start_date + "\n"
								+ "		AND F.forecast_date <= " + end_date + "\n";
				
				if (args.length > 1)
				{
					Path path = Paths.get(args[1]);
					script += "		AND F.source = '" + path.toAbsolutePath().toString() + "'\n";
				}
				
				script += "GROUP BY FR.lead_time;";
				
				try {
					connection = Database.get_connection();
					Statement query = connection.createStatement();
					query.setFetchSize(SystemConfig.instance().get_fetch_size());
					ResultSet results = query.executeQuery(script);
					String variable_id = String.valueOf(Variable.get_variable_id(variable));
					script = "SELECT R.measurement, FR.measurements\n"
							+ "FROM Forecast F\n"
							+ "INNER JOIN Observation O\n"
							+ "		ON O.variable_id = F.variable_id\n"
							+ "INNER JOIN ForecastResult FR\n"
							+ "		ON F.forecast_id = FR.forecast_id\n"
							+ "INNER JOIN ObservationResult R\n"
							+ "		ON R.observation_id = O.observation_id\n"
							+ "			AND R.valid_date = F.forecast_date + (INTERVAL '1 hour' * FR.lead_time)\n"
							+ "WHERE F.variable_id = '" + variable_id + "'\n"
							+ "		AND F.forecast_date >= " + start_date + "\n"
							+ "		AND F.forecast_date <= " + end_date + "\n";
					
					if (args.length > 1)
					{
						Path path = Paths.get(args[1]);
						script += "		AND F.source = '" + path.toAbsolutePath().toString() + "'\n";
					}
							
					System.out.println("Farming out computations...");
					while (results.next())
					{
						lead = results.getInt("lead_time");
						lead_script = script + "		AND FR.lead_time = " + String.valueOf(lead) + ";";
						Callable<Double> computation = new FunctionRunner<Double, Double>(lead_script, 
																				  Metrics.calculateMeanError(), 
																				  (Double value) -> { 
																					  return value * 25.4;
																				  }); 

						Future<Double> future_computation = Executor.submit(computation);
						computed_errors.put(lead, future_computation);
					}					

					for (Integer lead_time : computed_errors.keySet())
					{
						errors.put(lead_time, computed_errors.get(lead_time).get());
					}
					
					System.out.println("Mean errors computed.");
					System.out.println("Mean Error:");
					for (Integer lead_time : errors.keySet())
					{
						System.out.print("\t");
						System.out.print(lead_time);
						System.out.print(" |\t");
						System.out.println(errors.get(lead_time));
					}
					
				} catch (SQLException | InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
				finally
				{
					if (connection != null)
					{
						try {
							Database.return_connection(connection);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
			else
			{
				System.out.println("Not enough arguments were passed.");
				System.out.println("*.jar getPairs <variable name> [<source name>]");
			}
		};
	}
	
	/**
	 * Creates the "systemMetrics" method
	 * @return A method that will display the available processors, the amount of free memory, the amount of maximum memory,
	 * and the total memory of the system.
	 */
	private static final Consumer<String[]> systemMetrics()
	{
		return (String[] args) -> {
			  /* Total number of processors or cores available to the JVM */
			  System.out.println("Available processors (cores): " + 
			  Runtime.getRuntime().availableProcessors());

			  /* Total amount of free memory available to the JVM */
			  System.out.println("Free memory (bytes): " + 
			  Runtime.getRuntime().freeMemory());

			  /* This will return Long.MAX_VALUE if there is no preset limit */
			  long maxMemory = Runtime.getRuntime().maxMemory();
			  /* Maximum amount of memory the JVM will attempt to use */
			  System.out.println("Maximum memory (bytes): " + 
			  (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

			  /* Total memory currently in use by the JVM */
			  System.out.println("Total memory (bytes): " + 
			  Runtime.getRuntime().totalMemory());
		};
	}
	
	/**
	 * Creates the "describeNetCDF" method
	 * @return A method that will read a NetCDF file from the given path and output details about global attributes,
	 * variable details, variable attributes, and sample data.
	 */
	private static final Consumer<String[]> describeNetCDF()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				NetCDFReader reader = new NetCDFReader(args[0]);
				reader.output_variables();
			}
			else
			{
				System.out.println("A path is needed to describe the data. Please pass that in as the first argument.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
	
	/**
	 * Creates the "queryNetCDF" method
	 * @return A method that will accept a filename, a variable name, and optional positional details. Using those parameters,
	 * the given NetCDF file will be opened and the a value from the optional position for the given variable will be printed to
	 * the screen.
	 */
	private static final Consumer<String[]> queryNetCDF()
	{
		return (String[] args) -> {
			if (args.length > 1)
			{
				String filename = args[0];
				String variable_name = args[1];
				int[] variable_args = new int[args.length - 2];
				for (int index = 2; index < args.length; ++index)
				{
					variable_args[index-2] = Integer.parseInt(args[index]);
				}
				NetCDFReader reader = new NetCDFReader(filename);
				reader.print_query(variable_name, variable_args);
			}
			else
			{
				System.out.println("There are not enough parameters to query the netcdf.");
				System.out.println("usage: queryNetCDF <filename> <variable> [index0, index1,...indexN]");
			}
		};
	}
	
	private static final Consumer<String[]> describeProjects()
	{
		return (String[] args) -> {
			System.out.println();
			System.out.println();
			System.out.println("The configured projects are:");
			System.out.println();
			System.out.println();
			for (Project project : ProjectConfig.get_projects())
			{
				System.out.println(project.toString());
			}
		};
	}
}
