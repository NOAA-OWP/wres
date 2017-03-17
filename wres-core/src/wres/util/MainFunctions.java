/**
 * 
 */
package wres.util;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import wres.concurrency.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import wres.reading.BasicSource;
import wres.reading.SourceReader;
import wres.collections.Pair;
import java.util.concurrent.Future;
import wres.concurrency.FunctionRunner;
import wres.concurrency.Metrics;;
/**
 * @author ctubbs
 *
 */
public class MainFunctions {

	private static final Map<String, Consumer<String[]>> functions = createMap();
	
	public static final boolean has_operation(String operation)
	{
		return functions.containsKey(operation.toLowerCase());
	}
	
	public static final void call(String operation, String[] args)
	{
		operation = operation.toLowerCase();
		functions.get(operation).accept(args);
	}
	
	private static final Map<String, Consumer<String[]>> createMap()
	{
		Map<String, Consumer<String[]>> prototypes = new HashMap<String, Consumer<String[]>>();
		
		prototypes.put("loadwaterdata", loadWaterData());
		prototypes.put("copywaterdata", copyWaterData());
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
		prototypes.put("concurrentmeanerror", concurrentMeanError());
		prototypes.put("systemmetrics", systemMetrics());
		
		return prototypes;
	}
	
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
	
	private static final Consumer<String[]> loadWaterData()
	{
		return (String[] args) ->
		{
			if (args.length > 0)
			{
				try {
					BasicSource source = SourceReader.get_source(args[0]);
					source.read();
					source.print();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to load data. Please pass that in as the first argument.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}

	private static final Consumer<String[]> copyWaterData()
	{
		return (String[] args) ->{
			if (args.length > 0)
			{	String filename = args[0].replaceFirst("\\.", "_copy.");
				try {
					BasicSource source = SourceReader.get_source(args[0]);
					source.read();
					source.write(filename);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to copy data. Please pass that in as the first argument.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
	
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
	
	private static final Consumer<String[]> connectToDB()
	{
		return (String[] args) -> {
			try {
				ResultSet result = Database.execute_for_result("SELECT version();");
				System.out.println(result.getString("version"));
				System.out.println("Successfully connected to the database");
			} catch (SQLException e) {
				System.out.println("Could not connect to database because:");
				e.printStackTrace();
			}
		};
	}
	
	private static final Consumer<String[]> getPairs()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				String variable = args[0];
				
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
					ResultSet results = Database.execute_for_result(script);
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
			}
			else
			{
				System.out.println("Not enough arguments were passed.");
				System.out.println("*.jar getPairs <variable name> [<source name>]");
			}
		};
	}
	
	private static final Consumer<String[]> meanError()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				String variable = args[0];
				
				String script = "SELECT F.forecast_date + INTERVAL '1 hour' * FR.lead_time AS forecast_time,\n"
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
								+ "			AND R.valid_date + INTERVAL '1 hour' * 6 = F.forecast_date + INTERVAL '1 hour' * FR.lead_time\n"
								+ "INNER JOIN Variable V\n"
								+ "		ON V.variable_id = F.variable_id\n"
								+ "WHERE V.variable_name = '" + variable + "'\n";
				
				if (args.length > 1)
				{
					Path path = Paths.get(args[1]);
					script += "		AND F.source = '" + path.toAbsolutePath().toString() + "'\n";
				}
				
				script += "ORDER BY forecast_time, FR.lead_time;";
				
				try {
					ResultSet results = Database.execute_for_result(script);
					System.out.println("Pair data is now in memory!");
					TreeMap<Integer, Pair<Double, Integer>> errors = new TreeMap<Integer, Pair<Double, Integer>>();
					TreeMap<Integer, Double> mean = new TreeMap<Integer, Double>();
					Double total = 0.0;
					Integer lead_time = 0;
					Float[] raw_ensembles = null;
					while (results.next())
					{
						raw_ensembles = (Float[])results.getArray("measurements").getArray();

						float observed_value = results.getFloat("measurement");
						lead_time = results.getInt("lead_time");
						
						for (int ensemble_index = 0; ensemble_index < raw_ensembles.length; ++ensemble_index)
						{
							total += (raw_ensembles[ensemble_index]*25.4) - (observed_value*25.4);
						}
						
						if (errors.containsKey(lead_time))
						{
							errors.get(lead_time).item_one += total;
							errors.get(lead_time).item_two += raw_ensembles.length;
						}
						else
						{							
							errors.put(lead_time, new Pair<Double, Integer>(total, raw_ensembles.length));
							mean.put(lead_time, 0.0);
						}
						
						total = 0.0;
					}
					
					System.out.println("Data has been distributed...");
					
					
					for (Integer lead : errors.keySet()) 
					{
						Pair<Double, Integer> error = errors.get(lead);
						mean.put(lead, error.item_one/error.item_two);
					}
					
					System.out.println("Mean errors computed.");
					
					System.out.println("Mean Error:");
					for (int time : errors.keySet())
					{
						System.out.print("\t");
						System.out.print(time);
						System.out.print(" |\t");
						System.out.println(mean.get(time));
					}
					
					
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("Not enough arguments were passed.");
				System.out.println("*.jar getPairs <variable name> [<source name>]");
			}
		};
	}
	
	private static final Consumer<String[]> concurrentMeanError()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				String variable = args[0];
				String start_date = "'1/1/1800'";
				String end_date = "'12/1/2400'";
				int lead = 0;
				String lead_script = "";
				
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
					ResultSet results = Database.execute_for_result(script);
					//ExecutorService executor = Executors.newFixedThreadPool(4);

					script = "SELECT R.measurement, FR.measurements\n"
							+ "FROM Forecast F\n"
							+ "INNER JOIN Variable V\n"
							+ "		ON V.variable_id = F.variable_id\n"
							+ "INNER JOIN Observation O\n"
							+ "		ON O.variable_id = F.variable_id\n"
							+ "INNER JOIN ForecastResult FR\n"
							+ "		ON F.forecast_id = FR.forecast_id\n"
							+ "INNER JOIN ObservationResult R\n"
							+ "		ON R.observation_id = O.observation_id\n"
							+ "			AND R.valid_date = F.forecast_date + (INTERVAL '1 hour' * FR.lead_time)\n"
							+ "WHERE V.variable_name = '" + variable + "'\n"
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
						//Future<Double> future_computation = executor.submit(computation);
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

					//Executor.shutdown();
					//while (!executor.isTerminated()) {}
					
				} catch (SQLException | InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("Not enough arguments were passed.");
				System.out.println("*.jar getPairs <variable name> [<source name>]");
			}
		};
	}
	
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
}
