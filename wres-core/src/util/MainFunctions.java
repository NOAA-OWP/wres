package util;

import java.io.File;
import java.nio.file.Paths;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import concurrency.Downloader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import wres.io.concurrency.Executor;
import wres.io.concurrency.ForecastSaver;
import java.util.function.Consumer;

import wres.io.config.ProjectSettings;
import wres.io.data.caching.Variables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.config.Projects;
import wres.io.config.SystemSettings;
import wres.io.reading.BasicSource;
import wres.io.reading.ConfiguredLoader;
import wres.io.reading.ReaderFactory;
import wres.io.config.specification.MetricSpecification;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.config.specification.ProjectSpecification;

import java.util.concurrent.Future;

import wres.io.concurrency.MetricTask;
import wres.io.concurrency.ObservationSaver;
import wres.io.data.caching.MeasurementUnits;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.DataFactory;
import wres.io.grouping.LeadResult;
import wres.io.reading.fews.PIXMLReader;
import wres.io.utilities.Database;
import wres.util.*;

/**
 * @author ctubbs
 *
 */
public final class MainFunctions
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MainFunctions.class);

	// Clean definition of the newline character for the system
	private static final String newline = System.lineSeparator();

	// Mapping of String names to corresponding methods
	private static final Map<String, Consumer<String[]>> functions = createMap();

	public static void shutdown()
	{
		Executor.complete();
		Database.shutdown();
		ProgressMonitor.deactivate();
	}

	/**
	 * Determines if there is a method for the requested operation
	 *
	 * @param operation The desired operation to perform
	 * @return True if there is a method mapped to the operation name
	 */
	public static boolean hasOperation (String operation) {
		return functions.containsKey(operation.toLowerCase());
	}

	/**
	 * Executes the operation with the given list of arguments
	 *
	 * @param operation The name of the desired method to call
	 * @param args      The desired arguments to use when calling the method
	 */
	public static void call (String operation, String[] args) {
		operation = operation.toLowerCase();
		functions.get(operation).accept(args);
		shutdown();
	}

	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
	private static Map<String, Consumer<String[]>> createMap () {
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
		prototypes.put("executeproject", executeProject());
		prototypes.put("refreshtestdata", refreshTestData());
		prototypes.put("refreshstatistics", refreshStatistics());

		return prototypes;
	}

	/**
	 * Creates the "print_commands" method
	 *
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
	 *
	 * @return Method that will attempt to save the file at the given path to the database as forecasts
	 */
	private static Consumer<String[]> saveForecast()
	{
		return (String[] args) -> {
			if (args.length > 0) {
				try {

					ForecastSaver saver = new ForecastSaver(Paths.get(args[0]).toAbsolutePath().toString());
					saver.setOnRun(ProgressMonitor.onThreadStartHandler());
					saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
					Future<?> task = Executor.execute(saver);

					task.get();

					Executor.complete();
					Database.shutdown();

					System.out.println();
					System.out.println("All forecast saving operations complete. Please verify data.");
				}
				catch (Exception e) {
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
	 *
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
					final File[] files = new File(directory).listFiles((File file) -> {
						return file.isFile() && file.getName().endsWith(".xml") || file.getName().endsWith("gz") || file.getName().endsWith("nc");
					});

					File[] filteredFiles = wres.util.Collections.removeAll(files, (File file) -> {
						final String name = file.getName();
						return name.endsWith(".gz") && wres.util.Collections.find(files, (File other) -> {
							return other.getName().equalsIgnoreCase(name.substring(0, name.length() - 3));
						}) != null;
					});

					System.out.println();
					System.out.println(String.format("Attempting to save all files in '%s' as forecasts to the database... (This might take a little while)", args[0]));
					System.out.println();

					ArrayList<Future<?>> tasks = new ArrayList<>();

					for (File file : filteredFiles) {
						ForecastSaver saver = new ForecastSaver(file.getAbsolutePath());
						saver.setOnRun(ProgressMonitor.onThreadStartHandler());
						saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
						tasks.add(Executor.execute(saver));
					}

					for (Future<?> task : tasks) {
						task.get();
					}

					PIXMLReader.saveLeftoverForecasts();

					if (tasks.size() > 0)
                    {
                        System.out.println("Refreshing the statistics in the database...");
                        Database.refreshStatistics();
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
	 *
	 * @return Method that will attempt to save a file to the database as an observation
	 */
	private static Consumer<String[]> saveObservation()
	{
		return (String[] args) -> {
			if (args.length > 0) {
				try {
					BasicSource source = ReaderFactory.getReader(args[0]);
					System.out.println(String.format("Attempting to save '%s' to the database...", args[0]));
					source.saveObservation();
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
	 *
	 * @return Method that will attempt to save all files in a directory to the database as observations
	 */
	private static Consumer<String[]> saveObservations () {
		return (String[] args) -> {
			if (args.length > 0) {
				try {
					String directory = args[0];
					File[] files = new File(directory).listFiles();
					ArrayList<Future<?>> tasks = new ArrayList<>();
					System.out.println(String.format("Attempting to save all files in '%s' as observations to the database...", args[0]));
					for (File file : files != null ? files : new File[0]) {
						tasks.add(Executor.execute(new ObservationSaver(file.getAbsolutePath())));
					}

					for (Future<?> task : tasks) {
						task.get();
					}

					Executor.complete();
					Database.shutdown();
					System.out.println("All observation saving operations complete. Please verify data.");
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
			else {
				System.out.println("A path to a directory is needed to save data. Please pass that in as the first argument.");
				System.out.println("usage: saveObservations <directory path>");
			}
		};
	}

	/**
	 * Creates the "connectToDB" method
	 *
	 * @return method that will attempt to connect to the database to prove that a connection is possible. The version of the connected database will be printed.
	 */
	private static Consumer<String[]> connectToDB () {
		return (String[] args) -> {
			try {
				String version = Database.getResult("Select version() AS version_detail", "version_detail");
				System.out.println(version);
				System.out.println("Successfully connected to the database");
			}
			catch (SQLException e) {
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
			}
			catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				System.out.println("");
				System.out.println("Cleaning up the ForecastEnsemble table...");
				Database.execute("VACUUM FULL ANALYZE wres.ForecastEnsemble;");
				Database.execute("REINDEX TABLE wres.ForecastEnsemble;");
				System.out.println("The ForecastEnsemble table has been refreshed.");
				System.out.println("");
			}
			catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				System.out.println("");
				System.out.println("Cleaning up the ForecastValue table...");
				Database.execute("VACUUM FULL ANALYZE wres.ForecastValue;");
				Database.execute("REINDEX TABLE wres.ForecastValue;");
				System.out.println("The ForecastValue table has been refreshed.");
				System.out.println("");
			}
			catch (SQLException e) {
				e.printStackTrace();
			}
		};
	}

	/**
	 * Creates the "systemMetrics" method
	 *
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
	 *
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
	 *
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
	 *
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
			for (ProjectSpecification project : ProjectSettings.getProjects()) {
				System.out.println(project.toString());
			}
		};
	}

	/**
	 * Creates the "flushDatabase" method
	 *
	 * @return A method that will remove all dynamic forecast, observation, and variable data from the database. Prepares the
	 * database for a cold start.
	 */
	private static Consumer<String[]> flushDatabase()
	{
		return (String[] args) -> {
			String script = "";
			script += "TRUNCATE wres.Observation;" + newline;
			script += "TRUNCATE wres.ForecastValue;" + newline;
			script += "TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;" + newline;
			script += "TRUNCATE wres.ForecastSource;" + newline;
			script += "TRUNCATE wres.Source RESTART IDENTITY CASCADE;" + newline;
			script += "TRUNCATE wres.Forecast RESTART IDENTITY CASCADE;" + newline;

			try {
				Database.execute(script);
			}
			catch (SQLException e) {
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
	 *
	 * @return A method that will remove all forecast data from the database.
	 */
	private static Consumer<String[]> flushForecasts()
	{
		return (String[] args) -> {
			String script = "";
			script += "TRUNCATE wres.ForecastSource;" + newline;
			script += "DELETE FROM wres.Source S" + newline;
			script += "WHERE NOT EXISTS (" + newline;
			script += "      SELECT 1" + newline;
			script += "      FROM wres.Observation O" + newline;
			script += "      WHERE O.source_id = S.source_id" + newline;
			script += ");" + newline;
			script += "TRUNCATE wres.ForecastValue;" + newline;
			script += "TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;" + newline;
			script += "TRUNCATE wres.Forecast RESTART IDENTITY CASCADE;" + newline;

			try {
				Database.execute(script);
			}
			catch (SQLException e) {
				System.err.println("WRES forecast data could not be removed from the database." + newline);
				System.err.println();
				System.err.println(script);
				System.err.println();
				e.printStackTrace();
			}
		};
	}

	private static Consumer<String[]> refreshTestData () {
		return (String[] args) -> {
			if (args.length >= 2) {
				String date = args[0];
				String range = args[1];

				if (!Arrays.asList("analysis_assim", "fe_analysis_assim", "fe_medium_range", "fe_short_range", "forcing_analysis_assim", "forcing_medium_range", "forcing_short_range", "long_range_mem1", "long_range_mem2", "long_range_mem3", "long_range_mem4", "medium_range", "short_range").contains(range.toLowerCase())) {
					System.err.println("The range of: '" + range + "' is not a valid range of data.");
					return;
				}

				int offset = 0;
				int hourIncrement;
				int cutoff;
				int current;
				boolean isAssim = false;
				boolean isLong = false;
				final boolean isForcing = Strings.contains(range, "fe") || Strings.contains(range, "forcing");

				Map<String, Future> downloadOperations = new TreeMap<>();

				String downloadPath = "testinput/sharedinput/";
				downloadPath += date;
				File downloadDirectory = new File(downloadPath);

				if (!downloadDirectory.exists()) {
					System.out.println("Attempting to create a directory for the dataset...");

					try {
						boolean directoriesMade = downloadDirectory.mkdirs();
						if (!directoriesMade) {
							System.err.println("A directory could not be created for the downloaded files.");
						}
					}
					catch (SecurityException exception) {
						System.err.println("You lack the permissions necessary to make the directory for this data.");
						System.err.println("You will need to get access to your data through other means.");
						return;
					}
				}

				if (Strings.contains(range, "long_range")) {
					hourIncrement = 6;
					current = 6;
					cutoff = 720;
					isLong = true;
				}
				else if (Strings.contains(range, "short_range")) {
					current = 1;
					hourIncrement = 1;
					cutoff = 15;
				}
				else if (Strings.contains(range, "medium_range")) {
					offset = 6;
					current = 1;
					hourIncrement = 3;
					cutoff = 240;
				}
				else {
					hourIncrement = 1;
					current = 0;
					cutoff = 11;
					isAssim = true;
				}

				while (current <= cutoff) {
					String address = "http://***REMOVED***dstore.***REMOVED***.***REMOVED***/nwm/nwm.";
					address += date;
					address += "/";
					address += range;
					address += "/";

					String filename = "nwm.t";

					if (isAssim) {
						if (current < 10) {
							filename += "0";
						}

						filename += String.valueOf(current);
					}
					else {
						if (offset < 10) {
							filename += "0";
						}

						filename += String.valueOf(offset);
					}

					filename += "z.";

					if (isLong) {
						filename += "long_range";
					}
					else {
						filename += range;
					}

					if (!isForcing) {
						filename += ".land";
					}

					if (isLong) {
						filename += "_";
						filename += Strings.extractWord(range, "\\d$");
					}

					filename += ".";

					if (!isAssim) {
						filename += "f";
						if (current < 100) {
							filename += "0";
							if (current < 10) {
								filename += "0";
							}
						}

						filename += String.valueOf(current);
					}
					else {
						filename += "tm00";

					}

					filename += ".conus.nc";

					if (date.compareTo("20170509") < 0) {
						filename += ".gz";
					}

					address += filename;

					Downloader downloadOperation = new Downloader(Paths.get(downloadDirectory.getAbsolutePath(), filename), address);
					downloadOperation.setOnRun(ProgressMonitor.onThreadStartHandler());
					downloadOperation.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
					downloadOperations.put(filename, Executor.execute(downloadOperation));

					current += hourIncrement;
				}

				for (Entry<String, Future> operation : downloadOperations.entrySet()) {
					try {
						operation.getValue().get();
					}
					catch (InterruptedException | ExecutionException e) {
						System.err.println("An error was encountered while attempting to complete the download for '" + operation.getKey() + "'.");
						e.printStackTrace();
					}
				}
			}
			else {
				System.out.println("There are not enough parameters to download updated netcdf data.");
				System.out.println("usage: refreshTestData <date> <range name>");
				System.out.println("Example: refreshTestData 20170508 long_range_mem4");
				System.out.println("Acceptable ranges are:");
				System.out.println("	analysis_assim");
				System.out.println("	fe_analysis_assim");
				System.out.println("	fe_medium_range");
				System.out.println("	fe_short_range");
				System.out.println("	forcing_analysis_assim");
				System.out.println("	forcing_medium_range");
				System.out.println("	forcing_short_range");
				System.out.println("	long_range_mem1");
				System.out.println("	long_range_mem2");
				System.out.println("	long_range_mem3");
				System.out.println("	long_range_mem4");
				System.out.println("	medium_range");
				System.out.println("	short_range");
			}
		};
	}

	/**
	 * Creates the "flushObservations" method
	 *
	 * @return A method that will remove all observation data from the database.
	 */
	private static Consumer<String[]> flushObservations()
	{
		return (String[] args) -> {
			String script;
			script = "TRUNCATE wres.Observation RESTART IDENTITY CASCADE;" + newline;
			script += "DELETE FROM wres.Source S" + newline;
			script += "WHERE NOT EXISTS (" + newline;
			script += "      SELECT 1" + newline;
			script += "      FROM wres.ForecastSource FS" + newline;
			script += "      WHERE FS.source_id = S.source_id" + newline;
			script += ");" + newline;

			try {
				Database.execute(script);
			}
			catch (SQLException e) {
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
	 *
	 * @return Prints the count and the first 10 pairs for all observations and forecasts for the passed in forecast variable,
	 * observation variable, and lead time
	 */
	private static Consumer<String[]> getPairs () {
		return (String[] args) -> {

			Connection connection = null;
			try {
				String forecastVariable = args[0];
				String observationVariable = args[1];
				String lead = args[2];
				String targetUnit = args[3];
				int targetUnitID = MeasurementUnits.getMeasurementUnitID(targetUnit);
				int observationVariableID = Variables.getVariableID(observationVariable, targetUnitID);
				int forecastVariableID = Variables.getVariableID(forecastVariable, targetUnitID);
				int forecastVariablePositionID;
				int observationVariablePositionID;

				List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
				String script;

				script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + observationVariableID + ";";
				observationVariablePositionID = Database.getResult(script, "variableposition_id");

				script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + forecastVariableID + ";";
				forecastVariablePositionID = Database.getResult(script, "variableposition_id");

				script = "";
				script += "WITH forecast_measurements AS (" + newline;
				script += "   SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + newline;
				script += "       array_agg(FV.forecasted_value * UC.factor) AS forecasts" + newline;
				script += "   FROM wres.Forecast F" + newline;
				script += "   INNER JOIN wres.ForecastEnsemble FE" + newline;
				script += "       ON F.forecast_id = FE.forecast_id" + newline;
				script += "   INNER JOIN wres.ForecastValue FV" + newline;
				script += "       ON FV.forecastensemble_id = FE.forecastensemble_id" + newline;
				script += "   INNER JOIN wres.UnitConversion UC" + newline;
				script += "       ON UC.from_unit = FE.measurementunit_id" + newline;
				script += "   WHERE lead = " + lead + newline;
				script += "       AND FE.variableposition_id = " + forecastVariablePositionID + newline;
				script += "       AND UC.to_unit = " + targetUnitID + newline;
				script += "   GROUP BY forecasted_date" + newline;
				script += ")" + newline;
				script += "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + newline;
				script += "FROM forecast_measurements FM" + newline;
				script += "INNER JOIN wres.Observation O" + newline;
				script += "   ON O.observation_time = FM.forecasted_date" + newline;
				script += "INNER JOIN wres.UnitConversion UC" + newline;
				script += "   ON UC.from_unit = O.measurementunit_id" + newline;
				script += "WHERE O.variableposition_id = " + observationVariablePositionID + newline;
				script += "   AND UC.to_unit = " + targetUnitID + newline;
				script += "ORDER BY FM.forecasted_date;";

				connection = Database.getConnection();
				ResultSet results = Database.getResults(connection, script);
				DataFactory valueFactory = wres.datamodel.DataFactory.instance();
				while (results.next()) {
					pairs.add(valueFactory.pairOf((double) results.getFloat("observation"), (Double[]) results.getArray("forecasts").getArray()));
				}

				System.out.println();
				System.out.println(pairs.size() + " pairs were retrieved!");
				System.out.println();

				for (int i = 0; i < 10; ++i) {
					String representation = pairs.get(i).toString();
					representation = representation.substring(0, Math.min(100, representation.length()));
					if (representation.length() == 100) {
						representation += "...";
					}
					System.out.println(representation);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				if (connection != null) {
					Database.returnConnection(connection);
				}
			}
		};
	}

	private static Consumer<String[]> refreshStatistics ()
	{
		return (String[] args) -> {
			Database.refreshStatistics();
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
	            ProjectSpecification foundProject = ProjectSettings.getProject(projectName);
	            Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> pairMapping;
	            
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
	
	private static Consumer<String[]> executeProject() {
	    return (String[] args) -> {
	        if (args.length > 0) {
	            String projectName = args[0];
	            String metricName = null;
	            
	            if (args.length > 1) {
	                metricName = args[1];
	            }
	            
	            ProjectSpecification project = ProjectSettings.getProject(projectName);
	            int ingestedFileCount = 0;
	            for (ProjectDataSpecification datasource : project.getDatasources())
	            {
	                System.err.println("Loading datasource information if it doesn't already exist...");
	                ConfiguredLoader dataLoader = new ConfiguredLoader(datasource);
	                ingestedFileCount += dataLoader.load();
	                System.err.println("The data from this dataset has been loaded to the database");
	                ProgressMonitor.resetMonitor();
	                System.err.println();
	            }

	            System.err.println("All data specified for this project should now be loaded.");

	            if (ingestedFileCount > 0)
                {
                    System.out.println("Refreshing the statistics in the database...");
                    Database.refreshStatistics();
                }
	            
	            Map<String, List<LeadResult>> results = new TreeMap<>();
	            Map<String, Future<List<LeadResult>>> futureResults = new TreeMap<>();
	            
	            if (metricName == null)
	            {
    	            for (int metricIndex = 0; metricIndex < project.metricCount(); ++metricIndex)
    	            {
    	                MetricSpecification specification = project.getMetric(metricIndex);
    	                MetricExecutor metric = new MetricExecutor(specification);
    	                metric.setOnRun(ProgressMonitor.onThreadStartHandler());
    	                metric.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
    	                System.err.println("Now executing the metric named: " + specification.getName());
    	                futureResults.put(specification.getName(), Executor.submit(metric));
    	            }
	            }
	            else
	            {
	                MetricSpecification specification = project.getMetric(metricName);
	                
	                if (specification != null)
	                {
	                    MetricExecutor metric = new MetricExecutor(specification);
                        metric.setOnRun(ProgressMonitor.onThreadStartHandler());
                        metric.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                        System.err.println("Now executing the metric named: " + specification.getName());
                        futureResults.put(specification.getName(), Executor.submit(metric));
	                }
	            }
	            
	            for (Entry<String, Future<List<LeadResult>>> entry : futureResults.entrySet())
	            {
	                try
                    {
                        results.put(entry.getKey(), entry.getValue().get());
                    }
                    catch(InterruptedException | ExecutionException e)
                    {
                        e.printStackTrace();
                    }
	            }
	            
	            System.out.println();
	            System.out.println("Project: " + projectName);
	            System.out.println();
	            
	            for (Entry<String, List<LeadResult>> entry : results.entrySet())
	            {
	                System.out.println();
	                System.out.println(entry.getKey());
	                System.out.println("--------------------------------------------------------------------------------------");
	                
	                for (LeadResult metricResult : entry.getValue())
	                {
	                    System.out.print(metricResult.getLead());
	                    System.out.print("\t\t|\t");
	                    System.out.println(metricResult.getResult());
	                }
	                
	                System.out.println();
	            }
	        }
	        else
	        {
	            System.err.println("There are not enough arguments to run 'executeProject'");
	            System.err.println("usage: executeProject <project name> [<metric name>]");
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
