package util;

import concurrency.Downloader;
import concurrency.ProjectExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metric.DefaultMetricInputFactory;
import wres.datamodel.metric.MetricInputFactory;
import wres.io.concurrency.*;
import wres.io.config.ProjectSettings;
import wres.io.config.SystemSettings;
import wres.io.config.specification.MetricSpecification;
import wres.io.config.specification.ProjectDataSpecification;
import wres.io.config.specification.ProjectSpecification;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.grouping.LeadResult;
import wres.io.reading.BasicSource;
import wres.io.reading.ConfiguredLoader;
import wres.io.reading.ReaderFactory;
import wres.io.reading.fews.PIXMLReader;
import wres.io.utilities.Database;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * @author ctubbs
 *
 */
public final class MainFunctions
{
	public static final Integer FAILURE = -1;
	public static final Integer SUCCESS = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MainFunctions.class);

	// Clean definition of the newline character for the system
	private static final String NEWLINE = System.lineSeparator();

	// Mapping of String names to corresponding methods
	private static final Map<String, Function<String[], Integer>> functions = createMap();

	public static void shutdown()
	{
		LOGGER.info("Shutting down the application...");
		Database.restoreAllIndices();
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
	public static boolean hasOperation (final String operation) {
		return functions.containsKey(operation.toLowerCase());
	}

	/**
	 * Executes the operation with the given list of arguments
	 *
	 * @param operation The name of the desired method to call
	 * @param args      The desired arguments to use when calling the method
	 */
	public static Integer call (String operation, final String[] args) {
		operation = operation.toLowerCase();
		final Integer result = functions.get(operation).apply(args);
		shutdown();
		return result;
	}

	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
	private static Map<String, Function<String[], Integer>> createMap () {
		final Map<String, Function<String[], Integer>> prototypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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
		prototypes.put("executeClassProject", new ProjectExecutor());
		prototypes.put("refreshtestdata", refreshTestData());
		prototypes.put("refreshstatistics", refreshStatistics());
		prototypes.put("ingestproject", ingestProject());
		prototypes.put("loadcoordinates", loadCoordinates());
		prototypes.put("builddatabase", buildDatabase());

		return prototypes;
	}

	/**
	 * Creates the "print_commands" method
	 *
	 * @return Method that prints all available commands by name
	 */
	private static Function<String[], Integer> printCommands()
	{
		return (final String[] args) -> {
			System.out.println("Available commands are:");
			for (final String command : functions.keySet())
			{
				System.out.println("\t" + command);
			}
			return SUCCESS;
		};
	}

	/**
	 * Creates the "saveForecast" method
	 *
	 * @return Method that will attempt to save the file at the given path to the database as forecasts
	 */
	private static Function<String[], Integer> saveForecast()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length > 0) {
				try {

					final ForecastSaver saver = new ForecastSaver(Paths.get(args[0]).toAbsolutePath().toString());
					saver.setOnRun(ProgressMonitor.onThreadStartHandler());
					saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
					final Future<?> task = Executor.execute(saver);

					task.get();

					Executor.complete();
					Database.shutdown();

					LOGGER.info("");
					LOGGER.info("All forecast saving operations complete. Please verify data.");
					result = SUCCESS;
				}
				catch (final Exception e) {
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
			return result;
		};
	}

	/**
	 * Creates the "saveForecasts" method
	 *
	 * @return Method that will attempt to save all files in the given directory to the database as forecasts
	 */
	private static Function<String[], Integer> saveForecasts()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length > 0)
			{
				try
				{
					final String directory = args[0];
					final File[] files = new File(directory).listFiles((final File file) -> {
						return file.isFile() && file.getName().endsWith(".xml") || file.getName().endsWith("gz") || file.getName().endsWith("nc");
					});

					final File[] filteredFiles = wres.util.Collections.removeAll(files, (final File file) -> {
						final String name = file.getName();
						return name.endsWith(".gz") && wres.util.Collections.find(files, (final File other) -> {
							return other.getName().equalsIgnoreCase(name.substring(0, name.length() - 3));
						}) != null;
					});

					LOGGER.info("");
					LOGGER.info(String.format("Attempting to save all files in '%s' as forecasts to the database... (This might take a little while)", args[0]));
					LOGGER.info("");

					final ArrayList<Future<?>> tasks = new ArrayList<>();


					for (final File file : filteredFiles) {
						final ForecastSaver saver = new ForecastSaver(file.getAbsolutePath());
						saver.setOnRun(ProgressMonitor.onThreadStartHandler());
						saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
						tasks.add(Executor.execute(saver));
					}

					for (final Future<?> task : tasks) {
						task.get();
					}

					PIXMLReader.saveLeftoverForecasts();

					LOGGER.info("Making sure all ingest tasks are complete...");

					Database.completeAllIngestTasks();

					if (tasks.size() > 0)
                    {
                        Database.refreshStatistics();
                    }

					LOGGER.info("");
					LOGGER.info("All forecast saving operations complete. Please verify data.");
					result = SUCCESS;
				}
				catch (final Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path to a directory is needed to save data. Please pass that in as the first argument.");
				System.out.println("usage: saveForecasts <directory path>");
			}
			return result;
		};
	}

	/**
	 * Creates the "saveObservation" method
	 *
	 * @return Method that will attempt to save a file to the database as an observation
	 */
	private static Function<String[], Integer> saveObservation()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length > 0) {
				try {
					final BasicSource source = ReaderFactory.getReader(args[0]);
					System.out.println(String.format("Attempting to save '%s' to the database...", args[0]));
					source.saveObservation();
					System.out.println("Database save operation completed. Please verify data.");
					Database.completeAllIngestTasks();
					result = SUCCESS;
				}
				catch (final Exception e)
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
			return result;
		};
	}

	/**
	 * Creates a Method that will attempt to save all files in a directory as observations
	 *
	 * @return Method that will attempt to save all files in a directory to the database as observations
	 */
	private static Function<String[], Integer> saveObservations () {
		return (final String[] args) -> {
			final Integer result = FAILURE;
			if (args.length > 0) {
				try {
					final String directory = args[0];
					final File[] files = new File(directory).listFiles();
					final ArrayList<Future<?>> tasks = new ArrayList<>();
					System.out.println(String.format("Attempting to save all files in '%s' as observations to the database...", args[0]));
					for (final File file : files != null ? files : new File[0]) {
						tasks.add(Executor.execute(new ObservationSaver(file.getAbsolutePath())));
					}

					for (final Future<?> task : tasks) {
						task.get();
					}

					Executor.complete();
					Database.shutdown();
					System.out.println("All observation saving operations complete. Please verify data.");
					return result;
				}
				catch (final Exception e) {
					e.printStackTrace();
				}
			}
			else {
				System.out.println("A path to a directory is needed to save data. Please pass that in as the first argument.");
				System.out.println("usage: saveObservations <directory path>");
			}
			return result;
		};
	}

	/**
	 * Creates the "connectToDB" method
	 *
	 * @return method that will attempt to connect to the database to prove that a connection is possible. The version of the connected database will be printed.
	 */
	private static Function<String[], Integer> connectToDB () {
		return (final String[] args) -> {
			Integer result = FAILURE;
			try {
				final String version = Database.getResult("Select version() AS version_detail", "version_detail");
				LOGGER.info(version);
				LOGGER.info("Successfully connected to the database");
				result = SUCCESS;
			}
			catch (final SQLException e) {
				LOGGER.error("Could not connect to database because:");
				LOGGER.error(Strings.getStackTrace(e));
			}
			catch (final RuntimeException exception)
            {
                LOGGER.error(Strings.getStackTrace(exception));
            }
            return result;
		};
	}
	
	private static Function<String[], Integer> refreshForecasts()
	{
		return (final String[] args) -> {
			Integer result = SUCCESS;
			try {
                LOGGER.info("");
                LOGGER.info("Cleaning up the Forecast table...");
				Database.execute("VACUUM ANALYZE wres.Forecast;");
				Database.execute("REINDEX TABLE wres.Forecast;");
				LOGGER.info("The Forecast table has been refreshed.");
                LOGGER.info("");
			}
			catch (final SQLException e) {
				LOGGER.error(Strings.getStackTrace(e));
                result = FAILURE;
			}

			try {
                LOGGER.info("");
                LOGGER.info("Cleaning up the ForecastEnsemble table...");
				Database.execute("VACUUM ANALYZE wres.ForecastEnsemble;");
				Database.execute("REINDEX TABLE wres.ForecastEnsemble;");
                LOGGER.info("The ForecastEnsemble table has been refreshed.");
                LOGGER.info("");
			}
			catch (final SQLException e) {
                LOGGER.error(Strings.getStackTrace(e));
                result = FAILURE;
			}

			try {
                LOGGER.info("");
                LOGGER.info("Cleaning up the ForecastValue table...");
				Database.execute("VACUUM ANALYZE wres.ForecastValue;");
				Database.execute("REINDEX TABLE wres.ForecastValue;");
                LOGGER.info("The ForecastValue table has been refreshed.");
                LOGGER.info("");
			}
			catch (final SQLException e) {
                LOGGER.error(Strings.getStackTrace(e));
				result = FAILURE;
			}

			return result;
		};
	}

	/**
	 * Creates the "systemMetrics" method
	 *
	 * @return A method that will display the available processors, the amount of free memory, the amount of maximum memory,
	 * and the total memory of the system.
	 */
	private static Function<String[], Integer> systemMetrics()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;

			try {
                LOGGER.info(Strings.getSystemStats());

                // Add white space
                LOGGER.info("");

                result = SUCCESS;
            }
            catch (final RuntimeException e)
            {
                LOGGER.error(Strings.getStackTrace(e));
            }
            return result;
		};
	}

	/**
	 * Creates the "describeNetCDF" method
	 *
	 * @return A method that will read a NetCDF file from the given path and output details about global attributes,
	 * variable details, variable attributes, and sample data.
	 */
	private static Function<String[], Integer> describeNetCDF()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length > 0)
			{
			    try {
                    final NetCDFReader reader = new NetCDFReader(args[0]);
                    reader.output_variables();
                    result = SUCCESS;
                }
                catch (final RuntimeException e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
			}
			else
			{
				System.out.println("A path is needed to describe the data. Please pass that in as the first argument.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
			return result;
		};
	}

	/**
	 * Creates the "queryNetCDF" method
	 *
	 * @return A method that will accept a filename, a variable name, and optional positional details. Using those parameters,
	 * the given NetCDF file will be opened and the a value from the optional position for the given variable will be printed to
	 * the screen.
	 */
	private static Function<String[], Integer> queryNetCDF()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length > 1)
			{
			    try {
                    final String filename = args[0];
                    final String variable_name = args[1];
                    final int[] variable_args = new int[args.length - 2];
                    for (int index = 2; index < args.length; ++index) {
                        variable_args[index - 2] = Integer.parseInt(args[index]);
                    }
                    final NetCDFReader reader = new NetCDFReader(filename);
                    reader.print_query(variable_name, variable_args);
                    result = SUCCESS;
                }
                catch (final RuntimeException e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
			}
			else
			{
				System.out.println("There are not enough parameters to query the netcdf.");
				System.out.println("usage: queryNetCDF <filename> <variable> [index0, index1,...indexN]");
			}
			return result;
		};
	}

	/**
	 * Creates the "describeProjects" method
	 *
	 * @return A method that will print out details about every found project in the path indicated by the system
	 * configuration in a more human readable format.
	 */
	private static Function<String[], Integer> describeProjects()
	{
		return (final String[] args) -> {
			Integer result = SUCCESS;

			System.out.println();
			System.out.println();
			System.out.println("The configured projects are:");
			System.out.println();
			System.out.println();

			try {
				for (final ProjectSpecification project : ProjectSettings.getProjects()) {
					System.out.println(project.toString());
				}
			}
			catch (final RuntimeException exception)
			{
				result = FAILURE;
			}
			return result;
		};
	}

	/**
	 * Creates the "flushDatabase" method
	 *
	 * @return A method that will remove all dynamic forecast, observation, and variable data from the database. Prepares the
	 * database for a cold start.
	 */
	private static Function<String[], Integer> flushDatabase()
	{
		return (final String[] args) -> {
			Integer result = SUCCESS;

			try {
                StringBuilder builder = new StringBuilder();

                Connection connection = null;
                ResultSet results = null;
                boolean partitionsLoaded;

                builder.append("SELECT 'DROP TABLE IF EXISTS '||n.nspname||'.'||c.relname||' CASCADE;'").append(NEWLINE);
                builder.append("FROM pg_catalog.pg_class c").append(NEWLINE);
                builder.append("INNER JOIN pg_catalog.pg_namespace n").append(NEWLINE);
                builder.append("    ON N.oid = C.relnamespace").append(NEWLINE);
                builder.append("WHERE relchecks > 0").append(NEWLINE);
                builder.append("    AND nspname = 'wres' OR nspname = 'partitions'").append(NEWLINE);
                builder.append("    AND relkind = 'r';");

                try {
                    connection = Database.getConnection();
                    results = Database.getResults(connection, builder.toString());

                    builder = new StringBuilder();
                    partitionsLoaded = true;

                    while (results.next()) {
                        builder.append(results.getString(1)).append(NEWLINE);
                    }
                }
                catch (final SQLException e) {
                    LOGGER.error(Strings.getStackTrace(e));
                    throw e;
                }

                if (!partitionsLoaded) {
                    builder = new StringBuilder();
                }

                builder.append("TRUNCATE wres.ForecastSource;").append(NEWLINE);
                builder.append("TRUNCATE wres.ForecastValue;").append(NEWLINE);
                builder.append("TRUNCATE wres.Observation;").append(NEWLINE);
                builder.append("TRUNCATE wres.Source RESTART IDENTITY CASCADE;").append(NEWLINE);
                builder.append("TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;").append(NEWLINE);
                builder.append("TRUNCATE wres.Forecast RESTART IDENTITY CASCADE;").append(NEWLINE);
                builder.append("TRUNCATE wres.Variable RESTART IDENTITY CASCADE;").append(NEWLINE);

                try {
                    Database.execute(builder.toString());
                }
                catch (final SQLException e) {
                    LOGGER.error("WRES data could not be removed from the database." + NEWLINE);
                    LOGGER.error("");
                    LOGGER.error(builder.toString());
                    LOGGER.error("");
                    LOGGER.error(Strings.getStackTrace(e));
                    throw e;
                }
            }
            catch (final Exception e)
            {
                LOGGER.error(Strings.getStackTrace(e));
                result = FAILURE;
            }
			return result;
		};
	}

	/**
	 * Creates the "flushForecasts" method
	 *
	 * @return A method that will remove all forecast data from the database.
	 */
	private static Function<String[], Integer> flushForecasts()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
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
				result = SUCCESS;
			}
			catch (final Exception e) {
				LOGGER.error("WRES forecast data could not be removed from the database." + NEWLINE);
                LOGGER.error("");
                LOGGER.error(script);
				LOGGER.error("");
                LOGGER.error(Strings.getStackTrace(e));
			}

			return result;
		};
	}

	private static Function<String[], Integer> refreshTestData () {
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length >= 2) {
				final String date = args[0];
				final String range = args[1];

                int cutoff = -1;

                if (args.length >= 3 && Strings.isNumeric(args[2]))
                {
                    cutoff = Integer.parseInt(args[2]);
                }


				if (!Arrays.asList("analysis_assim",
								   "fe_analysis_assim",
								   "fe_medium_range",
								   "fe_short_range",
								   "forcing_analysis_assim",
								   "forcing_medium_range",
								   "forcing_short_range",
								   "long_range_mem1",
								   "long_range_mem2",
								   "long_range_mem3",
								   "long_range_mem4",
								   "medium_range",
								   "short_range").contains(range.toLowerCase())) {
                    LOGGER.error("The range of: '" + range + "' is not a valid range of data.");
					return FAILURE;
				}

				ProgressMonitor.deactivate();

				try {
                    int offset = 0;
                    int hourIncrement;
                    int current;
                    boolean isAssim = false;
                    boolean isLong = false;
                    final boolean isForcing = Strings.contains(range, "fe") || Strings.contains(range, "forcing");

                    final Map<String, Future> downloadOperations = new TreeMap<>();

                    String downloadPath = "testinput/sharedinput/";
                    downloadPath += date;
                    final File downloadDirectory = new File(downloadPath);

                    if (!downloadDirectory.exists()) {
                        LOGGER.trace("Attempting to create a directory for the dataset...");

                        try {
                            final boolean directoriesMade = downloadDirectory.mkdirs();
                            if (!directoriesMade) {
                                LOGGER.warn("A directory could not be created for the downloaded files.");
                            }
                        }
                        catch (final SecurityException exception) {
                            LOGGER.error("You lack the permissions necessary to make the directory for this data.");
                            LOGGER.error("You will need to get access to your data through other means.");
                            throw exception;
                        }
                    }

                    if (Strings.contains(range, "long_range")) {
                        hourIncrement = 6;
                        current = 6;

                        if (cutoff == -1) {
                            cutoff = 720;
                        }

                        isLong = true;
                    }
                    else if (Strings.contains(range, "short_range")) {
                        current = 1;
                        hourIncrement = 1;

                        if (cutoff == -1)
                        {
                            cutoff = 15;
                        }
                    }
                    else if (Strings.contains(range, "medium_range")) {
                        offset = 0;
                        current = 3;
                        hourIncrement = 3;

                        if (cutoff == -1) {
                            cutoff = 240;
                        }
                    }
                    else {
                        hourIncrement = 1;
                        current = 0;

                        if (cutoff == -1) {
                            cutoff = 11;
                        }

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

                        final Downloader downloadOperation = new Downloader(Paths.get(downloadDirectory.getAbsolutePath(), filename), address);
                        downloadOperation.setOnRun(ProgressMonitor.onThreadStartHandler());
                        downloadOperation.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                        downloadOperations.put(filename, Executor.execute(downloadOperation));

                        current += hourIncrement;
                    }

                    for (final Entry<String, Future> operation : downloadOperations.entrySet()) {
                        try {
                            operation.getValue().get();
                        }
                        catch (InterruptedException | ExecutionException e) {
                            LOGGER.error("An error was encountered while attempting to complete the download for '" + operation.getKey() + "'.");
                            throw e;
                        }
                    }
                    result = SUCCESS;
                }
                catch (final Exception e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                    result = FAILURE;
                }
			}
			else {
				LOGGER.warn("There are not enough parameters to download updated netcdf data.");
                LOGGER.warn("usage: refreshTestData <date> <range name>");
                LOGGER.warn("Example: refreshTestData 20170508 long_range_mem4");
                LOGGER.warn("Acceptable ranges are:");
                LOGGER.warn("	analysis_assim");
                LOGGER.warn("	fe_analysis_assim");
                LOGGER.warn("	fe_medium_range");
                LOGGER.warn("	fe_short_range");
                LOGGER.warn("	forcing_analysis_assim");
                LOGGER.warn("	forcing_medium_range");
                LOGGER.warn("	forcing_short_range");
                LOGGER.warn("	long_range_mem1");
                LOGGER.warn("	long_range_mem2");
                LOGGER.warn("	long_range_mem3");
                LOGGER.warn("	long_range_mem4");
                LOGGER.warn("	medium_range");
                LOGGER.warn("	short_range");
			}

			return result;
		};
	}

	/**
	 * Creates the "flushObservations" method
	 *
	 * @return A method that will remove all observation data from the database.
	 */
	private static Function<String[], Integer> flushObservations()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			String script;
			script = "TRUNCATE wres.Observation RESTART IDENTITY CASCADE;" + NEWLINE;
			script += "DELETE FROM wres.Source S" + NEWLINE;
			script += "WHERE NOT EXISTS (" + NEWLINE;
			script += "      SELECT 1" + NEWLINE;
			script += "      FROM wres.ForecastSource FS" + NEWLINE;
			script += "      WHERE FS.source_id = S.source_id" + NEWLINE;
			script += ");" + NEWLINE;

			try {
				Database.execute(script);
				result = SUCCESS;
			}
			catch (final Exception e) {
				LOGGER.error("WRES Observation data could not be removed from the database." + NEWLINE);
                LOGGER.error("");
				LOGGER.error(script);
				LOGGER.error("");
				LOGGER.error(Strings.getStackTrace(e));
			}
			return result;
		};
	}

	/**
	 * Creates the 'getPairs' function
	 *
	 * @return Prints the count and the first 10 pairs for all observations and forecasts for the passed in forecast variable,
	 * observation variable, and lead time
	 */
	private static Function<String[], Integer> getPairs () {
		return (final String[] args) -> {

			Integer result = FAILURE;
			
			Connection connection = null;
			try {
				final String forecastVariable = args[0];
				final String observationVariable = args[1];
				final String lead = args[2];
				final String targetUnit = args[3];
				final int targetUnitID = MeasurementUnits.getMeasurementUnitID(targetUnit);
				final int observationVariableID = Variables.getVariableID(observationVariable, targetUnitID);
				final int forecastVariableID = Variables.getVariableID(forecastVariable, targetUnitID);
				int forecastVariablePositionID;
				int observationVariablePositionID;

				final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
				String script;

				script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + observationVariableID + ";";
				observationVariablePositionID = Database.getResult(script, "variableposition_id");

				script = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = " + forecastVariableID + ";";
				forecastVariablePositionID = Database.getResult(script, "variableposition_id");

				script = "";
				script += "WITH forecast_measurements AS (" + NEWLINE;
				script += "   SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + NEWLINE;
				script += "       array_agg(FV.forecasted_value * UC.factor) AS forecasts" + NEWLINE;
				script += "   FROM wres.Forecast F" + NEWLINE;
				script += "   INNER JOIN wres.ForecastEnsemble FE" + NEWLINE;
				script += "       ON F.forecast_id = FE.forecast_id" + NEWLINE;
				script += "   INNER JOIN wres.ForecastValue FV" + NEWLINE;
				script += "       ON FV.forecastensemble_id = FE.forecastensemble_id" + NEWLINE;
				script += "   INNER JOIN wres.UnitConversion UC" + NEWLINE;
				script += "       ON UC.from_unit = FE.measurementunit_id" + NEWLINE;
				script += "   WHERE lead = " + lead + NEWLINE;
				script += "       AND FE.variableposition_id = " + forecastVariablePositionID + NEWLINE;
				script += "       AND UC.to_unit = " + targetUnitID + NEWLINE;
				script += "   GROUP BY forecasted_date" + NEWLINE;
				script += ")" + NEWLINE;
				script += "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + NEWLINE;
				script += "FROM forecast_measurements FM" + NEWLINE;
				script += "INNER JOIN wres.Observation O" + NEWLINE;
				script += "   ON O.observation_time = FM.forecasted_date" + NEWLINE;
				script += "INNER JOIN wres.UnitConversion UC" + NEWLINE;
				script += "   ON UC.from_unit = O.measurementunit_id" + NEWLINE;
				script += "WHERE O.variableposition_id = " + observationVariablePositionID + NEWLINE;
				script += "   AND UC.to_unit = " + targetUnitID + NEWLINE;
				script += "ORDER BY FM.forecasted_date;";

				connection = Database.getConnection();
				final ResultSet results = Database.getResults(connection, script);
				//JBr: replace DataFactory with with MetricInputFactory
				MetricInputFactory inFactory = DefaultMetricInputFactory.getInstance();
				while (results.next()) {
					pairs.add(inFactory.pairOf((double) results.getFloat("observation"), (Double[]) results.getArray("forecasts").getArray()));
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
				result = SUCCESS;
			}
			catch (final Exception e) {
				e.printStackTrace();
			}
			finally {
				if (connection != null) {
					Database.returnConnection(connection);
				}
			}
			return result;
		};
	}

	private static Function<String[], Integer> refreshStatistics ()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			try {
                Database.refreshStatistics();
                result = SUCCESS;
            }
            catch (final Exception e)
            {
                LOGGER.error(Strings.getStackTrace(e));
            }
            return result;
		};
	}

	private static Function<String[], Integer> getProjectPairs()
	{
	    return (final String[] args) -> {
			Integer result = FAILURE;
	        if (args.length > 1)
	        {
                try
                {
                    final String projectName = args[0];
                    final String metricName = args[1];
                    final int printLimit = 100;
                    int printCount = 0;
                    final int totalLimit = 10;
                    int totalCount = 0;
                    final ProjectSpecification foundProject = ProjectSettings.getProject(projectName);
                    Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> pairMapping;

                    if (foundProject == null)
                    {
                        System.err.println("There is not a project named '" + projectName + "'");
                        System.err.println("Pairs could not be created because there wasn't a specification.");
                        return result;
                    }

                    final MetricSpecification metric = foundProject.getMetric(metricName);

                    if (metric == null)
                    {
                        System.err.println("There is not a metric named '" + metricName + "' in the project '" + projectName + '"');
                        System.err.println("Pairs could not be created because there wasn't a specification.");
                        return result;
                    }

                    pairMapping = metric.getPairs();
                    
                    for (final Integer leadKey : pairMapping.keySet())
                    {
                        System.out.println("\tLead Time: " + leadKey);
                        for (final PairOfDoubleAndVectorOfDoubles pair : pairMapping.get(leadKey))
                        {
                            System.out.print("\t\t");
                            final String representation = pair.toString().substring(0, Math.min(120, pair.toString().length()));
                            System.out.println(representation);
                            
                            printCount++;
                            
                            if (printCount >= printLimit)
                            {
                                break;
                            }
                        }
                        
                        totalCount++;
                        printCount = 0;
                        
                        /*if (totalCount >= totalLimit)
                        {
                            break;
                        }*/
                    }
					result = SUCCESS;
                }
                catch(final Exception e)
                {
                    e.printStackTrace();
                }
	        }
	        else
	        {
	            System.err.println("There are not enough arguments to run 'getProjectPairs'");
	            System.err.println("usage: getProjectPairs <project name> <metric name>");
	        }
	        return result;
	    };
	}
	
	private static Function<String[], Integer> executeProject() {
	    return (final String[] args) -> {
			Integer result = FAILURE;
	        if (args.length > 0) {
	            try {
                    final String projectName = args[0];
                    String metricName = null;

                    if (args.length > 1) {
                        metricName = args[1];
                    }

                    final ProjectSpecification project = ProjectSettings.getProject(projectName);
                    final List<Future> ingestOperations = new ArrayList<>();

                    Database.suspendAllIndices();

                    for (final ProjectDataSpecification datasource : project.getDatasources()) {
                        LOGGER.info("Loading datasource information if it doesn't already exist...");
                        final ConfiguredLoader dataLoader = new ConfiguredLoader(datasource);
                        try {
                            ingestOperations.addAll(dataLoader.load());
                        }
                        catch (final IOException e) {
                            LOGGER.error(Strings.getStackTrace(e));
                        }
                    }

                    if (ingestOperations.size() > 0) {
                        for (final Future operation : ingestOperations) {
                            try {
                                operation.get();
                            }
                            catch (final InterruptedException e) {
                                e.printStackTrace();
                            }
                            catch (final ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    ProgressMonitor.resetMonitor();
					LOGGER.info("");
                    LOGGER.info("Restoring all suspended indices in the database...");
                    LOGGER.info("");
                    Database.restoreAllIndices();

                    final Map<String, List<LeadResult>> results = new TreeMap<>();
                    final Map<String, Future<List<LeadResult>>> futureResults = new TreeMap<>();

                    if (metricName == null) {
                        for (int metricIndex = 0; metricIndex < project.metricCount(); ++metricIndex) {
                            final MetricSpecification specification = project.getMetric(metricIndex);
                            final MetricTask metric = new MetricTask(specification, null);
                            metric.setOnRun(ProgressMonitor.onThreadStartHandler());
                            metric.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                            System.err.println("Now executing the metric named: " + specification.getName());
                            futureResults.put(specification.getName(), Executor.submit(metric));
                        }
                    }
                    else {
                        final MetricSpecification specification = project.getMetric(metricName);

                        if (specification != null) {
                            final MetricTask metric = new MetricTask(specification, null);
                            metric.setOnRun(ProgressMonitor.onThreadStartHandler());
                            metric.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                            System.err.println("Now executing the metric named: " + specification.getName());
                            futureResults.put(specification.getName(), Executor.submit(metric));
                        }
                    }

                    for (final Entry<String, Future<List<LeadResult>>> entry : futureResults.entrySet()) {
                        try {
                            results.put(entry.getKey(), entry.getValue().get());
                        }
                        catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                    }

                    System.out.println();
                    System.out.println("Project: " + projectName);
                    System.out.println();

                    for (final Entry<String, List<LeadResult>> entry : results.entrySet()) {
                        System.out.println();
                        System.out.println(entry.getKey());
                        System.out.println("--------------------------------------------------------------------------------------");

                        for (final LeadResult metricResult : entry.getValue()) {
                            System.out.print(metricResult.getLead());
                            System.out.print("\t\t|\t");
                            System.out.println(metricResult.getResult());
                        }

                        System.out.println();
                    }
                    result = SUCCESS;
                }
                catch (final Exception e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                    result = FAILURE;
                }
	        }
	        else
	        {
	            System.err.println("There are not enough arguments to run 'executeProject'");
	            System.err.println("usage: executeProject <project name> [<metric name>]");
	        }
	        return result;
	    };
    }

    private static Function<String[], Integer> ingestProject() {
        return (final String[] args) -> {
            Integer result = FAILURE;
            if (args.length > 0) {
                try {
                    final String projectName = args[0];

                    final ProjectSpecification project = ProjectSettings.getProject(projectName);
                    final List<Future> ingestOperations = new ArrayList<>();

                    Database.suspendAllIndices();

                    for (final ProjectDataSpecification datasource : project.getDatasources()) {
                        LOGGER.info("Loading datasource information if it doesn't already exist...");
                        final ConfiguredLoader dataLoader = new ConfiguredLoader(datasource);
                        try {
                            ingestOperations.addAll(dataLoader.load());
                        }
                        catch (final IOException e) {
                            LOGGER.error(Strings.getStackTrace(e));
                        }
                    }

                    if (ingestOperations.size() > 0) {
                        for (final Future operation : ingestOperations) {
                            try {
                                operation.get();
                            }
                            catch (final InterruptedException e) {
                                e.printStackTrace();
                            }
                            catch (final ExecutionException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                    /*LOGGER.info("Restoring all suspended indices in the database...");
                    Database.restoreAllIndices();*/

                    result = SUCCESS;
                }
                catch (Exception e)
                {
                    LOGGER.error(Strings.getStackTrace(e));
                    result = FAILURE;
                }
            }
            else
            {
                System.err.println("There are not enough arguments to run 'executeProject'");
                System.err.println("usage: executeProject <project name> [<metric name>]");
            }
            return result;
        };
    }

    private static Function<String[], Integer> loadCoordinates() {
		return (final String[] args) -> {
			Integer result = FAILURE;

			if (args.length > 0) {
				try
				{
                    // Assign path to NetCDF file
                    String filePath = args[0];

                    if (Files.notExists(Paths.get(filePath)))
                    {
                        throw new IOException("There is not a NetCDF file at the indicated path.");
                    }

					NetcdfFile file = NetcdfFile.open(args[0]);

                    // Make sure that NetCDF file has the required coordinate variables (can't rely on the isCoordinate attribute)
					if (!NetCDF.hasVariable(file, "x") || !NetCDF.hasVariable(file, "y"))
					{
						throw new IOException("The NetCDF file at: '" + args[0] + "' lacks the proper X and Y coodinates.");
					}

                    // Check to see if datum exists; if not, add it
                    // TODO: Add the datum checks
                    final int customSRID = 900914;

					final String copyHeader = "wres.NetCDFCoordinate (x_position, y_position, geographic_coordinate, resolution)";
					final String insertHeader = "INSERT INTO wres.NetCDFCoordinate (x_position, y_position, geographic_coordinate, resolution) VALUES ";
					final String delimiter = "|";
					final short tempResolution = 1000;
					StringBuilder builder = new StringBuilder(insertHeader);
					int copyCount = 0;

					List<Future> copyOperations = new ArrayList<>();

                    // Loop through a full join across all contained x and y values
                    Variable xCoordinates = NetCDF.getVariable(file, "x");
					Variable yCoordinates = NetCDF.getVariable(file, "y");

					int xLength = xCoordinates.getDimension(0).getLength();
					int yLength = yCoordinates.getDimension(0).getLength();

					Array xValues = xCoordinates.read();
					Array yValues = yCoordinates.read();

					int currentXIndex = 0;
					int currentYIndex = 0;
					for (; currentXIndex < xLength; ++currentXIndex) {
						currentYIndex = 0;

						for (; currentYIndex < yLength; ++currentYIndex) {

							if (copyCount > 0)
							{
								builder.append(", ");
							}

							builder.append("(");
							builder.append(currentXIndex).append(", ");
							builder.append(currentYIndex).append(", ");
							builder.append("ST_Transform(ST_SetSRID(ST_MakePoint(").
									append(xValues.getDouble(currentXIndex)).
										   append(",").
										   append(yValues.getDouble(currentYIndex)).
										   append("), ").
										   append(customSRID).append("), 4326)::point")
								   .append(", ");
							builder.append(tempResolution).append(")");

							copyCount++;

							if (copyCount >= SystemSettings.getMaximumCopies()) {
								LOGGER.trace("The copy count now exceeds the maximum allowable copies, so the values are being sent to save.");
								SQLExecutor sqlExecutor = new SQLExecutor(builder.toString());
								sqlExecutor.setOnRun(ProgressMonitor.onThreadStartHandler());
								sqlExecutor.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
								LOGGER.trace("Sending coordinate values to the database executor to copy...");
								copyOperations.add(Database.execute(sqlExecutor));
								builder = new StringBuilder(insertHeader);
								copyCount = 0;
							}
						}
					}

					for (Future copy : copyOperations) {
						copy.get();
					}

                    // Execute a query inserting all coordinates by using PostGIS to convert coordinates from the indicated
                    //      projection to the one WGS84 (ESPG:4326)
                    // If the correct datum of the source doesn't exist, create it
                    // Insert rows into the database dictating array positions from both x and y array positions and
                    //      a coordinate object
                    // i.e.     | x_position    | y_position    |   coordinate              |
                    //          |   2000        | 1098          |   POINT(-118.0, 20.07)    |

                    result = SUCCESS;
                }
                catch (Exception e) {
                    LOGGER.error(Strings.getStackTrace(e));
                    result = FAILURE;
                }
            }
            else
            {
                LOGGER.error("There are not enough arguments to run 'loadCoordinates'");
                LOGGER.error("usage: loadCoordinates <Path to NetCDF File containing coordinate information>");
            }

			return result;
		};
	}

	private static Function<String[], Integer> buildDatabase() {
		return (String[] args) -> {
			Integer result = FAILURE;

			Database.buildInstance();

			return result;
		};
	}
}
