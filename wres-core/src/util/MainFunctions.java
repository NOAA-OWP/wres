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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import concurrency.Executor;
import concurrency.ForecastSaver;
import java.util.function.Consumer;
import java.util.function.Function;

import collections.Pair;
import reading.BasicSource;
import reading.SourceReader;
import config.ProjectConfig;
import config.data.MetricSpecification;
import config.data.ProjectSpecification;
import collections.RealCollection;

import java.util.concurrent.Future;
import concurrency.FunctionRunner;
import concurrency.Metrics;
import concurrency.ObservationSaver;
import data.caching.MeasurementCache;
import data.caching.Variable;
import data.caching.VariableCache;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * @author ctubbs
 *
 */
@SuppressWarnings("deprecation")
public final class MainFunctions {

	// Clean definition of the newline character for the system
	private static final String newline = System.lineSeparator();
	
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
		Executor.complete();
		Database.shutdown();
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
		prototypes.put("querynetcdf", queryNetCDF());
		prototypes.put("commands", print_commands());
		prototypes.put("--help", print_commands());
		prototypes.put("-h", print_commands());
		prototypes.put("meanerror", meanError());
		prototypes.put("systemmetrics", systemMetrics());
		prototypes.put("saveobservations", saveObservations());
		prototypes.put("saveforecasts", saveForecasts());
		prototypes.put("describeprojects", describeProjects());
		prototypes.put("flushdatabase", flushDatabase());
		prototypes.put("flushforecasts", flushForecasts());
		prototypes.put("flushobservations", flushObservations());
		prototypes.put("refreshforecasts", refreshForecasts());
		prototypes.put("getpairs", getPairs());
		prototypes.put("getprojectpairs", getProjectPairs());
		
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
					File[] files = new File(directory).listFiles((File file) -> {
						return file.isFile() && file.getName().endsWith(".xml");
					});
					
					System.out.println();
					System.out.println(String.format("Attempting to save all files in '%s' as forecasts to the database... (This might take a little while)", args[0]));
					System.out.println();
					
					ArrayList<Future<?>> tasks = new ArrayList<Future<?>>();
					
					for (File file : files)
					{
						tasks.add(Executor.execute(new ForecastSaver(file.getAbsolutePath())));
					}
					
					for (Future<?> task : tasks)
					{
						task.get();
					}
					
					System.out.println();
					System.out.println(tasks.size() + " files were theoretically saved to the database. Closing now... (This might take a little while)");
					System.out.println();

					Executor.complete();
					Database.shutdown();
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
					ArrayList<Future<?>> tasks = new ArrayList<Future<?>>();
					System.out.println(String.format("Attempting to save all files in '%s' as observations to the database...", args[0]));
					for (File file : files)
					{
						tasks.add(Executor.execute(new ObservationSaver(file.getAbsolutePath())));
					}
					
					for (Future<?> task : tasks)
					{
						task.get();
					}
					
					Executor.complete();
					Database.shutdown();
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
				String version = Database.getResult("Select version() AS version_detail", "version_detail"); 
				System.out.println(version);
				System.out.println("Successfully connected to the database");
			} catch (SQLException e) {
				System.out.println("Could not connect to database because:");
				e.printStackTrace();
			}
		};
	}
	
	private static final Consumer<String[]> refreshForecasts()
	{
		return (String[] args) -> {
			try {
				System.out.println("");
				System.out.println("Cleaning up the Forecast table...");
				Database.execute("VACUUM FULL ANALYZE wres.Forecast;");
				Database.execute("REINDEX TABLE wres.Forecast;");
				System.out.println("The Forecast table has been refreshed.");
				System.out.println("");
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			try {
				System.out.println("");
				System.out.println("Cleaning up the ForecastEnsemble table...");
				Database.execute("VACUUM FULL ANALYZE wres.ForecastEnsemble;");
				Database.execute("REINDEX TABLE wres.ForecastEnsemble;");
				System.out.println("The ForecastEnsemble table has been refreshed.");
				System.out.println("");
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			try {
				System.out.println("");
				System.out.println("Cleaning up the ForecastValue table...");
				Database.execute("VACUUM FULL ANALYZE wres.ForecastValue;");
				Database.execute("REINDEX TABLE wres.ForecastValue;");
				System.out.println("The ForecastValue table has been refreshed.");
				System.out.println("");
			} catch (SQLException e) {
				e.printStackTrace();
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
					connection = Database.getConnection();
					ResultSet results = Database.getResults(connection, script);
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
							Database.returnConnection(connection);
						} catch (SQLException e) {
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
			Runtime runtime = Runtime.getRuntime();
		
			// Add white space
			System.out.println();
			
			/* Total number of processors or cores available to the JVM */
			System.out.println("Available processors (cores):\t" + 
			runtime.availableProcessors());
			
			/**
			 * Function to whittle down and describe the memory in a human readable format
			 * (i.e. not represented in raw number of bytes
			 */
			Function<Long, String> describeMemory = (Long memory) -> {
				String memoryUnit = " bytes";
				Double floatingMemory = memory.doubleValue();
				
				// Convert to KB if necessary
				if (floatingMemory > 1000)
				{
					floatingMemory = floatingMemory / 1000.0;
					memoryUnit = " KB";
				}
				
				// Convert to MB if Necessary
				if (floatingMemory > 1000)
				{
					floatingMemory = floatingMemory / 1000.0;
					memoryUnit = " MB";
				}
				
				// Convert to GB if necessary
				if (floatingMemory > 1000)
				{
					floatingMemory = floatingMemory / 1000.0;
					memoryUnit = " GB";
				}
				
				// Convert to TB if necessary
				if (floatingMemory > 1000)
				{
					floatingMemory = floatingMemory / 1000.0;
					memoryUnit = " TB";
				}
				
				return floatingMemory + memoryUnit;
			};
			
			// Theoretical amount of free memory
			System.out.println("Free memory:\t\t\t" + describeMemory.apply(runtime.freeMemory()));
			
			/* This will return Long.MAX_VALUE if there is no preset limit */
			long maxMemory = runtime.maxMemory();
			
			/* Maximum amount of memory the JVM will attempt to use */
			System.out.println("Maximum available memory:\t" + 
							   (maxMemory == Long.MAX_VALUE ? "no limit" : describeMemory.apply(maxMemory)));
			
			/* Total memory currently in use by the JVM */
			System.out.println("Total memory in use:\t\t" + describeMemory.apply(runtime.totalMemory()));

			// Add white space
			System.out.println();
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
	
	/**
	 * Creates the "describeProjects" method
	 * @return A method that will print out details about every found project in the path indicated by the system
	 * configuration in a more human readable format.
	 */
	private static final Consumer<String[]> describeProjects()
	{
		return (String[] args) -> {
			System.out.println();
			System.out.println();
			System.out.println("The configured projects are:");
			System.out.println();
			System.out.println();
			for (ProjectSpecification project : ProjectConfig.getProjects())
			{
				System.out.println(project.toString());
			}
		};
	}
	
	/**
	 * Creates the "flushDatabase" method
	 * @return A method that will remove all dynamic forecast, observation, and variable data from the database. Prepares the
	 * database for a cold start.
	 */
	private static final Consumer<String[]> flushDatabase()
	{
		return (String[] args) -> {
			String script = "";
			script += "DELETE FROM wres.ForecastValue;" + newline;
			script += "DELETE FROM wres.ForecastEnsemble;" + newline;
			script += "DELETE FROM wres.Ensemble;" + newline;
			script += "DELETE FROM wres.Forecast;" + newline;
			script += "DELETE FROM wres.Observation;" + newline;
			script += "DELETE FROM wres.FeaturePosition;" + newline;
			script += "DELETE FROM wres.VariablePosition;" + newline;
			script += "DELETE FROM wres.Variable;" + newline;
			
			try {
				Database.execute(script);
			} catch (SQLException e) {
				System.err.println("WRES data could not be removed from the database." + newline);
				System.err.println();
				System.err.println(script);
				System.err.println();
				e.printStackTrace();
			}
		};
	}
	
	/**
	 * Creates the "flushForecasts" method
	 * @return A method that will remove all forecast data from the database.
	 */
	private static final Consumer<String[]> flushForecasts()
	{
		return (String[] args) -> {
			String script = "";
			script += "TRUNCATE wres.ForecastValue RESTART IDENTITY CASCADE;" + newline;
			script += "TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;" + newline;
			script += "TRUNCATE wres.Forecast RESTART IDENTITY CASCADE;" + newline;
			
			try {
				Database.execute(script);
			} catch (SQLException e) {
				System.err.println("WRES forecast data could not be removed from the database." + newline);
				System.err.println();
				System.err.println(script);
				System.err.println();
				e.printStackTrace();
			}
		};
	}
	
	/**
	 * Creates the "flushObservations" method
	 * @return A method that will remove all observation data from the database.
	 */
	private static final Consumer<String[]> flushObservations()
	{
		return (String[] args) -> {
			String script = "TRUNCATE wres.Observation RESTART IDENTITY CASCADE;" + newline;
			
			try {
				Database.execute(script);
			} catch (SQLException e) {
				System.err.println("WRES Observation data could not be removed from the database." + newline);
				System.err.println();
				System.err.println(script);
				System.err.println();
				e.printStackTrace();
			}
		};
	}
	
	/**
	 * Creates the 'getPairs' function
	 * @return Prints the count and the first 10 pairs for all observations and forecasts for the passed in forecast variable,
	 * observation variable, and lead time
	 */
	private static final Consumer<String[]> getPairs() {
	    return (String[] args) -> {
	        
	        Connection connection = null;
            try
            {
                String forecastVariable = args[0];
                String observationVariable = args[1];
                String lead = args[2];
                String targetUnit = args[3];
                int targetUnitID = MeasurementCache.getMeasurementUnitID(targetUnit);
                int observationVariableID = VariableCache.getVariableID(observationVariable, targetUnitID);
                int forecastVariableID = VariableCache.getVariableID(forecastVariable, targetUnitID);
                int forecastVariablePositionID = 0;
                int observationVariablePositionID = 0;
                
                ArrayList<Pair<Float, RealCollection>> pairs = new ArrayList<Pair<Float, RealCollection>>();
        
                String script = "";
                
                script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + observationVariableID + ";";
                observationVariablePositionID = Database.getResult(script, "variableposition_id");
                
                script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + forecastVariableID + ";";
                forecastVariablePositionID = Database.getResult(script, "variableposition_id");
                
                script = "";
                script +=   "WITH forecast_measurements AS (" + newline;
                script +=   "   SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + newline;
                script +=   "       array_agg(FV.forecasted_value * UC.factor) AS forecasts" + newline;
                script +=   "   FROM wres.Forecast F" + newline;
                script +=   "   INNER JOIN wres.ForecastEnsemble FE" + newline;
                script +=   "       ON F.forecast_id = FE.forecast_id" + newline;
                script +=   "   INNER JOIN wres.ForecastValue FV" + newline;
                script +=   "       ON FV.forecastensemble_id = FE.forecastensemble_id" + newline;
                script +=   "   INNER JOIN wres.UnitConversion UC" + newline;
                script +=   "       ON UC.from_unit = FE.measurementunit_id" + newline;
                script +=   "   WHERE lead = " + lead + newline;
                script +=   "       AND FE.variableposition_id = " + forecastVariablePositionID + newline;
                script +=   "       AND UC.to_unit = " + targetUnitID + newline;
                script +=   "   GROUP BY forecasted_date" + newline;
                script +=   ")" + newline;
                script +=   "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + newline;
                script +=   "FROM forecast_measurements FM" + newline;
                script +=   "INNER JOIN wres.Observation O" + newline;
                script +=   "   ON O.observation_time = FM.forecasted_date" + newline;
                script +=   "INNER JOIN wres.UnitConversion UC" + newline;
                script +=   "   ON UC.from_unit = O.measurementunit_id" + newline;
                script +=   "WHERE O.variableposition_id = " + observationVariablePositionID + newline;
                script +=   "   AND UC.to_unit = " + targetUnitID + newline;
                script +=   "ORDER BY FM.forecasted_date;";
                
                connection = Database.getConnection();
                ResultSet results = Database.getResults(connection, script);
                
                while (results.next()) {
                    Pair<Float, RealCollection> pair = new Pair<Float, RealCollection>();
                    pair.setItemOne(results.getFloat("observation"));
                    pair.setItemTwo(new RealCollection());
                    for(Double result : (Double[])results.getArray("forecasts").getArray()) {
                        pair.getItemTwo().add(result);
                    }
                    pairs.add(pair);
                }
                
                System.out.println();
                System.out.println(pairs.size() + " pairs were retrieved!");
                System.out.println();
                
                for (int i = 0; i < 10; ++i) {
                    String representation = pairs.get(i).toString();
                    representation = representation.substring(0, Math.min(100, representation.length()));
                    if (representation.length() == 100)
                    {
                        representation += "...";
                    }
                    System.out.println(representation);
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            } 
            finally 
            {
                if (connection != null) {
                    try
                    {
                        Database.returnConnection(connection);
                    }
                    catch(SQLException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
	    };
	}
	
	private static Consumer<String[]> getProjectPairs()
	{
	    return (String[] args) -> {
	        if (args.length > 1)
	        {
	            String projectName = args[0];
	            String metricName = args[1];
	            int printLimit = 100;
	            int printCount = 0;
	            int totalLimit = 10;
	            int totalCount = 0;
	            ProjectSpecification foundProject = ProjectConfig.getProject(projectName);
	            Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> pairMapping = null;
	            
	            if (foundProject == null)
	            {
	                System.err.println("There is not a project named '" + projectName + "'");
	                System.err.println("Pairs could not be created because there wasn't a specification.");
	                return;
	            }
	            
	            MetricSpecification metric = foundProject.getMetric(metricName);
	            
	            if (metric == null)
	            {
                    System.err.println("There is not a metric named '" + metricName + "' in the project '" + projectName + '"');
                    System.err.println("Pairs could not be created because there wasn't a specification.");
                    return;
	            }
	            
	            try
                {
                    pairMapping = metric.getPairs();
                    
                    for (Integer leadKey : pairMapping.keySet())
                    {
                        System.out.println("\tLead Time: " + leadKey);
                        for (PairOfDoubleAndVectorOfDoubles pair : pairMapping.get(leadKey))
                        {
                            System.out.print("\t\t");
                            String representation = pair.toString().substring(0, Math.min(120, pair.toString().length()));
                            System.out.println(representation);
                            
                            printCount++;
                            
                            if (printCount >= printLimit)
                            {
                                break;
                            }
                        }
                        
                        totalCount++;
                        printCount = 0;
                        
                        if (totalCount >= totalLimit)
                        {
                            break;
                        }
                    }
                    
                    System.out.println();
                    System.out.println(Utilities.getSystemStats());
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
	        }
	        else
	        {
	            System.err.println("There are not enough arguments to run 'getProjectPairs'");
	            System.err.println("usage: getProjectPairs <project name> <metric name>");
	        }
	    };
	}
}
