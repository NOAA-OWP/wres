package util;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.config.Projects;
import wres.io.config.SystemSettings;
import wres.io.reading.BasicSource;
import wres.io.reading.ConfiguredLoader;
import wres.io.reading.SourceReader;
import wres.io.config.specification.MetricSpecification;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.config.specification.ProjectSpecification;

import java.util.concurrent.Future;

import wres.io.concurrency.MetricTask;
import wres.io.concurrency.ObservationSaver;
import wres.io.data.caching.MeasurementCache;
import wres.io.data.caching.VariableCache;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.DataFactory;
import wres.io.grouping.LeadResult;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
public final class MainFunctions
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MainFunctions.class);

	// Clean definition of the newline character for the system
	private static final String NEWLINE = System.lineSeparator();
	
	// Mapping of String names to corresponding methods
	private static final Map<String, Consumer<String[]>> functions = createMap();
	
	/**
	 * Determines if there is a method for the requested operation
	 * @param operation The desired operation to perform
	 * @return True if there is a method mapped to the operation name
	 */
	public static boolean hasOperation(String operation)
	{
		return functions.containsKey(operation.toLowerCase());
	}
	
	/**
	 * Executes the operation with the given list of arguments
	 * 
	 * @param operation The name of the desired method to call
	 * @param args The desired arguments to use when calling the method
	 */
	public static void call(String operation, String[] args)
	{
		operation = operation.toLowerCase();
		functions.get(operation).accept(args);	
		Executor.complete();
		Database.shutdown();
	}
	
	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
	private static Map<String, Consumer<String[]>> createMap()
	{
		Map<String, Consumer<String[]>> prototypes = new HashMap<>();
		
		prototypes.put("describenetcdf", describeNetCDF());
		prototypes.put("connecttodb", connectToDB());
		prototypes.put("saveforecast", saveForecast());
		prototypes.put("saveobservation", saveObservation());
		prototypes.put("querynetcdf", queryNetCDF());
		prototypes.put("commands", printCommands());
		prototypes.put("--help", printCommands());
		prototypes.put("-h", printCommands());
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
        prototypes.put("executeproject", new ExecuteProject());
		
		return prototypes;
	}
	
	/**
	 * Creates the "print_commands" method
	 * @return Method that prints all available commands by name
	 */
	private static Consumer<String[]> printCommands()
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
	private static Consumer<String[]> saveForecast()
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
	private static Consumer<String[]> saveForecasts()
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
					
					ArrayList<Future<?>> tasks = new ArrayList<>();

					for (File file : files)
					{
					    ForecastSaver saver = new ForecastSaver(file.getAbsolutePath());
					    saver.setOnRun(ProgressMonitor.onThreadStartHandler());
					    saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
						tasks.add(Executor.execute(saver));
					}
					
					for (Future<?> task : tasks)
					{
						task.get();
					}

					Executor.complete();
					Database.shutdown();
					System.out.println();
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
	private static Consumer<String[]> saveObservation()
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
	private static Consumer<String[]> saveObservations()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				try
				{
					String directory = args[0];
					File[] files = new File(directory).listFiles();
					ArrayList<Future<?>> tasks = new ArrayList<>();
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
	private static Consumer<String[]> connectToDB()
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
	
	private static Consumer<String[]> refreshForecasts()
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
	
	/**
	 * Creates the "systemMetrics" method
	 * @return A method that will display the available processors, the amount of free memory, the amount of maximum memory,
	 * and the total memory of the system.
	 */
	private static Consumer<String[]> systemMetrics()
	{
		return (String[] args) -> {
		    System.out.println(Strings.getSystemStats());

			// Add white space
			System.out.println();
		};
	}
	
	/**
	 * Creates the "describeNetCDF" method
	 * @return A method that will read a NetCDF file from the given path and output details about global attributes,
	 * variable details, variable attributes, and sample data.
	 */
	private static Consumer<String[]> describeNetCDF()
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
	private static Consumer<String[]> queryNetCDF()
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
	private static Consumer<String[]> describeProjects()
	{
		return (String[] args) -> {
			System.out.println();
			System.out.println();
			System.out.println("The configured projects are:");
			System.out.println();
			System.out.println();
			for (ProjectSpecification project : Projects.getProjects())
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
	private static Consumer<String[]> flushDatabase()
	{
		return (String[] args) -> {
			String script = "";
			script += "TRUNCATE wres.Observation;" + NEWLINE;
			script += "TRUNCATE wres.ForecastValue;" + NEWLINE;
			script += "TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;" + NEWLINE;
			script += "TRUNCATE wres.ForecastSource;" + NEWLINE;
			script += "TRUNCATE wres.Source RESTART IDENTITY CASCADE;" + NEWLINE;
			script += "TRUNCATE wres.Forecast RESTART IDENTITY CASCADE;" + NEWLINE;
			
			try {
				Database.execute(script);
			} catch (SQLException e) {
				System.err.println("WRES data could not be removed from the database." + NEWLINE);
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
	private static Consumer<String[]> flushForecasts()
	{
		return (String[] args) -> {
			String script = "";
            script += "TRUNCATE wres.ForecastSource;" + NEWLINE;
			script += "DELETE FROM wres.Source S" + NEWLINE;
			script += "WHERE NOT EXISTS (" + NEWLINE;
			script += "      SELECT 1" + NEWLINE;
			script += "      FROM wres.Observation O" + NEWLINE;
			script += "      WHERE O.source_id = S.source_id" + NEWLINE;
			script += ");" + NEWLINE;
			script += "TRUNCATE wres.ForecastValue;" + NEWLINE;
			script += "TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;" + NEWLINE;
			script += "TRUNCATE wres.Forecast RESTART IDENTITY CASCADE;" + NEWLINE;
			
			try {
				Database.execute(script);
			} catch (SQLException e) {
				System.err.println("WRES forecast data could not be removed from the database." + NEWLINE);
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
	private static Consumer<String[]> flushObservations()
	{
		return (String[] args) -> {
		    String script = "";
			script = "TRUNCATE wres.Observation RESTART IDENTITY CASCADE;" + NEWLINE;
            script += "DELETE FROM wres.Source S" + NEWLINE;
            script += "WHERE NOT EXISTS (" + NEWLINE;
            script += "      SELECT 1" + NEWLINE;
            script += "      FROM wres.ForecastSource FS" + NEWLINE;
            script += "      WHERE FS.source_id = S.source_id" + NEWLINE;
            script += ");" + NEWLINE;
			
			try {
				Database.execute(script);
			} catch (SQLException e) {
				System.err.println("WRES Observation data could not be removed from the database." + NEWLINE);
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
	private static Consumer<String[]> getPairs() {
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
                
                List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
                String script = "";
                
                script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + observationVariableID + ";";
                observationVariablePositionID = Database.getResult(script, "variableposition_id");
                
                script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + forecastVariableID + ";";
                forecastVariablePositionID = Database.getResult(script, "variableposition_id");
                
                script = "";
                script +=   "WITH forecast_measurements AS (" + NEWLINE;
                script +=   "   SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + NEWLINE;
                script +=   "       array_agg(FV.forecasted_value * UC.factor) AS forecasts" + NEWLINE;
                script +=   "   FROM wres.Forecast F" + NEWLINE;
                script +=   "   INNER JOIN wres.ForecastEnsemble FE" + NEWLINE;
                script +=   "       ON F.forecast_id = FE.forecast_id" + NEWLINE;
                script +=   "   INNER JOIN wres.ForecastValue FV" + NEWLINE;
                script +=   "       ON FV.forecastensemble_id = FE.forecastensemble_id" + NEWLINE;
                script +=   "   INNER JOIN wres.UnitConversion UC" + NEWLINE;
                script +=   "       ON UC.from_unit = FE.measurementunit_id" + NEWLINE;
                script +=   "   WHERE lead = " + lead + NEWLINE;
                script +=   "       AND FE.variableposition_id = " + forecastVariablePositionID + NEWLINE;
                script +=   "       AND UC.to_unit = " + targetUnitID + NEWLINE;
                script +=   "   GROUP BY forecasted_date" + NEWLINE;
                script +=   ")" + NEWLINE;
                script +=   "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + NEWLINE;
                script +=   "FROM forecast_measurements FM" + NEWLINE;
                script +=   "INNER JOIN wres.Observation O" + NEWLINE;
                script +=   "   ON O.observation_time = FM.forecasted_date" + NEWLINE;
                script +=   "INNER JOIN wres.UnitConversion UC" + NEWLINE;
                script +=   "   ON UC.from_unit = O.measurementunit_id" + NEWLINE;
                script +=   "WHERE O.variableposition_id = " + observationVariablePositionID + NEWLINE;
                script +=   "   AND UC.to_unit = " + targetUnitID + NEWLINE;
                script +=   "ORDER BY FM.forecasted_date;";
                
                connection = Database.getConnection();
                ResultSet results = Database.getResults(connection, script);
                DataFactory valueFactory = wres.datamodel.DataFactory.instance();
                while (results.next()) {
                    pairs.add(valueFactory.pairOf((double)results.getFloat("observation"), (Double[])results.getArray("forecasts").getArray()));
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
                    Database.returnConnection(connection);
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
	            ProjectSpecification foundProject = Projects.getProject(projectName);
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
                    System.out.println(Strings.getSystemStats());
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

    private static class ExecuteProject implements Consumer<String[]>
    {
        @Override
        public void accept(String[] args)
        {
            if (args.length > 0) {
                final String projectName = args[0];
                String metricName = null;

                if (args.length > 1) {
                    metricName = args[1];
                }

                final ProjectSpecification project = Projects.getProject(projectName);

                final List<Future> fileIngestTasks = new ArrayList<>();

                try
                {
                    for (ProjectDataSpecification datasource : project.getDatasources())
                    {
                        LOGGER.info("Loading datasource information if it doesn't already exist...");
                        final ConfiguredLoader dataLoader = new ConfiguredLoader(datasource);
                        fileIngestTasks.addAll(dataLoader.load());
                        if (LOGGER.isInfoEnabled())
                        {
                            LOGGER.info("Queued " + fileIngestTasks.size()
                                        + " file(s) for ingest");
                        }
                        ProgressMonitor.resetMonitor();
                    }
                }
                catch (IOException ioe)
                {
                    LOGGER.error("When trying to ingest files:", ioe);
                    return;
                }

                try
                {
                    for (Future t : fileIngestTasks)
                    {
                        if (t != null)
                        {
                            t.get();
                        }
                        else
                        {
                            LOGGER.debug("Received a null object from ConfiguredLoader");
                        }
                    }
                }
                catch (InterruptedException ie)
                {
                    LOGGER.warn("Interrupted during ingest", ie);
                    Thread.currentThread().interrupt();
                }
                catch (ExecutionException ee)
                {
                    LOGGER.error("Execution failed", ee);
                    return;
                }

                LOGGER.info("The data from this dataset has been ingested into the database");

                LOGGER.info("All data specified for this project should now be loaded.");

                int maxThreadCount = SystemSettings.maximumThreadCount() / 2;
                if (maxThreadCount == 0)
                {
                    maxThreadCount = 1;
                }

                // A secondary executor for second-level tasks, should help
                // avoid the situation where task A is waiting for another
                // task B in the queue that won't be executed until
                // tasks in the executor (task A) complete (possible deadlock?)
                final ExecutorService secondLevelExecutor = Executors.newFixedThreadPool(maxThreadCount);

                final Map<String, List<LeadResult>> results = new TreeMap<>();
                final Map<String, Future<List<LeadResult>>> futureResults = new TreeMap<>();

                if (metricName == null)
                {
                    for (int metricIndex = 0; metricIndex < project.metricCount(); ++metricIndex)
                    {
                        final MetricSpecification specification = project.getMetric(metricIndex);
                        final MetricTask metric = new MetricTask(specification, secondLevelExecutor);
                        metric.setOnRun(ProgressMonitor.onThreadStartHandler());
                        metric.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                        if (LOGGER.isInfoEnabled())
                        {
                            LOGGER.info("Now executing the metric named: " + specification.getName());
                        }
                        futureResults.put(specification.getName(), Executor.submit(metric));
                    }
                }
                else
                {
                    MetricSpecification specification = project.getMetric(metricName);

                    if (specification != null)
                    {
                        final MetricTask metric = new MetricTask(specification, secondLevelExecutor);
                        metric.setOnRun(ProgressMonitor.onThreadStartHandler());
                        metric.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                        if (LOGGER.isInfoEnabled())
                        {
                            LOGGER.info("Now executing the metric named: " + specification.getName());
                        }
                        futureResults.put(specification.getName(), Executor.submit(metric));
                    }
                }

                for (Entry<String, Future<List<LeadResult>>> entry : futureResults.entrySet())
                {
                    try
                    {
                        results.put(entry.getKey(), entry.getValue().get());
                    }
                    catch(InterruptedException ie)
                    {
                        LOGGER.warn("Interrupted", ie);
                        secondLevelExecutor.shutdown();
                        Thread.currentThread().interrupt();
                    }
                    catch (ExecutionException e)
                    {
                        LOGGER.error("Execution failed", e);
                        secondLevelExecutor.shutdown();
                        return;
                    }
                }

                secondLevelExecutor.shutdown();

                if (LOGGER.isInfoEnabled())
                {
                    LOGGER.info("");
                    LOGGER.info("Project: {}", projectName);
                    LOGGER.info("");

                    for (Entry<String, List<LeadResult>> entry : results.entrySet())
                    {
                        LOGGER.info("");
                            LOGGER.info(entry.getKey());
                        LOGGER.info("--------------------------------------------------------------------------------------");

                        for (LeadResult metricResult : entry.getValue())
                        {
                            LOGGER.info(metricResult.getLead() + "\t\t|\t" + metricResult.getResult());
                        }

                        LOGGER.info("");
                    }
                }
            }
            else
            {
                LOGGER.error("There are not enough arguments to run 'executeProject'");
                LOGGER.error("usage: executeProject <project name> [<metric name>]");
            }
        }
    }
}
