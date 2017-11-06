package wres;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricInput;
import wres.io.Operations;
import wres.io.concurrency.Downloader;
import wres.io.concurrency.Executor;
import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.io.reading.ReaderFactory;
import wres.io.reading.SourceType;
import wres.io.utilities.Database;
import wres.io.retrieval.InputGenerator;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
final class MainFunctions
{
	public static final Integer FAILURE = -1;
	public static final Integer SUCCESS = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MainFunctions.class);

    // TODO: This is a dumb location/process for storing the path to a project. This needs to be refactored.
    private static String PROJECT_PATH = "";

    // TODO: Again, a dumb way of handling this; we need a solution for storing the project config as a string to send
    // to the execution log.
    public static void setProjectPath(String projectPath)
    {
        synchronized (PROJECT_PATH)
        {
            MainFunctions.PROJECT_PATH = projectPath;
        }
    }

    @Deprecated
    public static String getRawProject()
    {
        File projectFile = null;

        if (PROJECT_PATH != null)
        {
            projectFile = new File(PROJECT_PATH);
        }

        String rawProject = null;

        if (projectFile != null && projectFile.exists() && projectFile.isFile())
        {
            try {
                StringBuilder projectBuilder = new StringBuilder();
                Files.lines(projectFile.toPath().toAbsolutePath()).forEach((String line) -> {
                    projectBuilder.append(line).append(System.lineSeparator());
                });
                rawProject = projectBuilder.toString();
            }
            catch (IOException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }

        }

        return rawProject;
    }

	// Mapping of String names to corresponding methods
	private static final Map<String, Function<String[], Integer>> FUNCTIONS = createMap();

    static void shutdown()
	{
	    ProgressMonitor.deactivate();
	    LOGGER.info("");
		LOGGER.info("Shutting down the application...");
		wres.io.Operations.shutdown();
	}

    static void shutdownWithAbandon( long timeOut, TimeUnit timeUnit )
    {
        ProgressMonitor.deactivate();
        LOGGER.info("");
        LOGGER.info( "Forcefully shutting down the application (you may see some errors)..." );
        wres.io.Operations.shutdownWithAbandon( timeOut, timeUnit );
    }

	/**
	 * Determines if there is a method for the requested operation
	 *
	 * @param operation The desired operation to perform
	 * @return True if there is a method mapped to the operation name
	 */
	public static boolean hasOperation (final String operation)
    {
		return FUNCTIONS.containsKey(operation.toLowerCase());
	}

	/**
	 * Executes the operation with the given list of arguments
	 *
	 * @param operation The name of the desired method to call
	 * @param args      The desired arguments to use when calling the method
	 */
	public static Integer call (String operation, final String[] args) {
	    Integer result = FAILURE;
		operation = operation.toLowerCase();

		if (MainFunctions.hasOperation(operation))
		{
            result = FUNCTIONS.get(operation).apply(args);
        }

		return result;
	}

	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
	private static Map<String, Function<String[], Integer>> createMap () {
		final Map<String, Function<String[], Integer>> prototypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		prototypes.put("connecttodb", connectToDB());
		prototypes.put("commands", printCommands());
		prototypes.put("--help", printCommands());
		prototypes.put("-h", printCommands());
		prototypes.put("cleandatabase", cleanDatabase());
		prototypes.put("execute", new Control());
		prototypes.put("downloadtestdata", refreshTestData());
		prototypes.put("refreshdatabase", refreshDatabase());
		prototypes.put("loadcoordinates", loadCoordinates());
		prototypes.put("install", install());
		prototypes.put("ingest", ingest());
		prototypes.put("loadfeatures", loadFeatures());
		prototypes.put("killconnections", killWRESConnections());
		prototypes.put("testchecksum", testChecksum());
		prototypes.put( "savepairs", savePairs() );

		return prototypes;
	}

	private static Function<String[], Integer> killWRESConnections()
    {
        return (final String[] args) -> {
            Integer result = FAILURE;
            final String NEWLINE = System.lineSeparator();

            try
            {
                StringBuilder script = new StringBuilder();
                script.append("SELECT pg_cancel_backend(pid)").append(NEWLINE);
                script.append("FROM pg_stat_activity").append(NEWLINE);
                script.append("WHERE client_port != -1").append(NEWLINE);
                script.append("     AND application_name = 'PostgreSQL JDBC Driver'").append(NEWLINE);
                script.append("     AND state = 'idle';");
                Database.execute(script.toString());
                result = SUCCESS;
            }
            catch (SQLException e) {
                LOGGER.error("Orphaned WRES connections to the WRES database could not be canceled.");
                LOGGER.error(Strings.getStackTrace(e));
            }

            return result;
        };
    }

    private static Function<String[], Integer> testChecksum()
    {
        return (final String[] args) -> {
            int result = FAILURE;

            if (args.length >= 1)
            {
                try
                {
                    LOGGER.info( Strings.getMD5Checksum( args[0] ) );
                }
                catch ( IOException e )
                {
                    LOGGER.error(Strings.getStackTrace( e ));
                }
            }
            else
            {
                LOGGER.info("Performing checksum on random data...");
                byte[] raw = new byte[4096];
                Random random = new Random(  );
                random.nextBytes( raw );
                LOGGER.info(Strings.getMD5Checksum( raw ));
            }

            return result;
        };
    }

	/**
	 * Creates the "print_commands" method
	 *
	 * @return Method that prints all available commands by name
	 */
	private static Function<String[], Integer> printCommands()
	{
		return (final String[] args) -> {
			LOGGER.info("Available commands are:");
			for (final String command : FUNCTIONS.keySet())
			{
				LOGGER.info("\t{}", command);
			}
			return SUCCESS;
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
			boolean successfullyConnected = Operations.testConnection();
			if (successfullyConnected)
            {
                result = SUCCESS;
            }
            return result;
		};
	}

	/**
	 * Creates the "cleanDatabase" method
	 *
	 * @return A method that will remove all dynamic forecast, observation, and variable data from the database. Prepares the
	 * database for a cold start.
	 */
	private static Function<String[], Integer> cleanDatabase ()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
            boolean successfullyCleaned = Operations.cleanDatabase();
            if (successfullyCleaned)
            {
                result = SUCCESS;
            }
			return result;
		};
	}

	private static Function<String[], Integer> savePairs()
    {
        return (final String[] args) -> {
            Integer result = FAILURE;

            if (args.length >= 1)
            {
                try
                {
                    ProjectConfig projectConfig = ProjectConfigPlus.from( Paths.get( args[0] ) ).getProjectConfig();
                    MainFunctions.setProjectPath( args[0] );
                    Feature feature = projectConfig.getPair()
                                                   .getFeature()
                                                   .get( 0 );

                    InputGenerator generator = Operations.getInputs(projectConfig,
                                                                     feature );

                    List<Future<MetricInput<?>>> futures = new ArrayList<>(  );

                    for (Future<MetricInput<?>> inputFuture : generator)
                    {
                        futures.add( inputFuture );
                    }

                    futures.forEach( metricInputFuture -> {
                        try
                        {
                            metricInputFuture.get();
                        }
                        catch ( InterruptedException | ExecutionException e )
                        {
                            LOGGER.error( Strings.getStackTrace( e ) );
                        }
                    } );

                    result = SUCCESS;
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
            else
            {
                LOGGER.error( "usage: savePairs <path to project>" );
            }
            return result;
        };
    }

    /**
     * Spawns threads to download NWM data and store them in easy to reach
     * locations.
     * <b>NOTE:</b> This will only work for NetCDF data in the format from
     * the data store
     * @return The spawning function
     */
	private static Function<String[], Integer> refreshTestData () {
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length >= 3)
			{
			    // The date from which to pull all forecasts
			    final String date = args[0];

			    // The range/forecast type, i.e. "long range", "short range", etc
				final String range = args[1];

				// The category of data like "land" or "channel_rt"
                final String category = args[2];

                // The last hour of a forecast to download; allows a user to
                // limit the number of files to download.
                int cutoff = Integer.MAX_VALUE;

                // If there is input for the cutoff, set it
                if (args.length >= 4 && Strings.isNumeric(args[3]))
                {
                    cutoff = Integer.parseInt(args[3]);
                }

                // If the range that was passed is not valid, error out
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

				// If the category of data isn't valid, error out
				if (!Arrays.asList("land", "reservoir", "channel_rt", "terrain_rt").contains(category.toLowerCase()))
                {
                    LOGGER.error("The category of '{}' is not a valid category for this type of data.", category);
                    return  FAILURE;
                }

                // We don't need the progress monitor, so disable it
				ProgressMonitor.deactivate();

				try
                {
                    // The last z hour to pull. We might start at t00z, but a
                    // forecast may continue to t18z. The maxZTime will be
                    // 18 in this case
				    int maxZTime = 0;

				    // The step at which to increment forecast start times
				    int zIncrement = 1;

				    // The increment between forecasted values (in hours)
                    int hourIncrement;

                    // The current hourly time step into a forecast to download
                    int current;

                    // The listing of all files to download for a particular
                    // forecast lead time. For instance, if we are downloading
                    // the current lead time of 3, we'll want to pull the files
                    // for lead time 3 for all forecasts between t00z and t18z
                    List<String> filenames;

                    // Whether or not the data to pull is for analysis and
                    // assimilation
                    boolean isAssim = false;

                    // Whether or not this is long range data
                    boolean isLong = false;

                    // Whether or not this is forcing data
                    final boolean isForcing = Strings.contains(range, "fe") ||
                                              Strings.contains(range, "forcing");

                    // A listing of a asynchronous download operations
                    final Map<String, Future> downloadOperations = new TreeMap<>();

                    // The location for where to put the downloaded files
                    String downloadPath = SystemSettings.getNetCDFStorePath();

                    if (!downloadPath.endsWith( "/" ))
                    {
                        downloadPath += "/";
                    }

                    downloadPath += category;
                    downloadPath += "/";
                    downloadPath += range;
                    downloadPath += "/";
                    downloadPath += date;
                    downloadPath += "/";
                    final File downloadDirectory = new File(downloadPath);

                    // If the directory doesn't exist, create it
                    if (!downloadDirectory.exists())
                    {
                        LOGGER.trace("Attempting to create a directory for the dataset...");

                        try
                        {
                            final boolean directoriesMade = downloadDirectory.mkdirs();
                            if (!directoriesMade)
                            {
                                LOGGER.warn("A directory could not be created for the downloaded files.");
                            }
                        }
                        catch (final SecurityException exception)
                        {
                            LOGGER.error("You lack the permissions necessary to make the directory for this data.");
                            LOGGER.error("You will need to get access to your data through other means.");
                            throw exception;
                        }
                    }

                    // We know we're long range if it is in the name
                    if (Strings.contains(range, "long_range"))
                    {
                        // The time between values is 6 hours
                        hourIncrement = 6;

                        // The first value for a forecast is at lead time 6
                        current = 6;

                        // The are 6 hours between long range forecasts
                        zIncrement = 6;

                        // The max for long_range_mem1 is 0; there will just be
                        // a bunch of failed pulls
                        maxZTime = 6;

                        // The last lead is 720. Set the cutoff to either
                        // 720 or the value entered by the user. If the user
                        // entered a value greater than 720, defer to 720 since
                        // nothing greater is valid
                        cutoff = Math.min(cutoff, 720);

                        // Declare that the operation is for long range values
                        isLong = true;
                    }
                    else if (Strings.contains(range, "short_range"))
                    {
                        // The first value is at a lead time of 1
                        current = 1;

                        // The time between forecasts is 1 hour
                        hourIncrement = 1;

                        // The last forecast to pull is at t23z
                        maxZTime = 23;

                        // The last lead is 18. Set the cutoff to either
                        // 18 or the value entered by the user. If the user
                        // entered a value greater than 18, defer to 18 since
                        // nothing greater is valid
                        cutoff = Math.min(cutoff, 18);
                    }
                    else if (Strings.contains(range, "medium_range"))
                    {
                        // The first value is at lead time 3
                        current = 3;

                        // There are 3 hours between each forecasted value
                        hourIncrement = 3;

                        // There are 6 hours between forecasts
                        zIncrement = 6;

                        // The last forecast occurs at t12z
                        maxZTime = 12;

                        // The last lead is 240. Set the cutoff to either
                        // 18 or the value entered by the user. If the user
                        // entered a value greater than 240, defer to 240 since
                        // nothing greater is valid
                        cutoff = Math.min(cutoff, 240);
                    }
                    else
                    {
                        // There is a single hour between values
                        hourIncrement = 1;

                        // The first value occurs at hour 0
                        current = 0;

                        // Simulations are generated 23 times in a day
                        maxZTime = 23;

                        // There is a single hour between simulation
                        // generation runs
                        zIncrement = 1;

                        // The cutoff is hard coded to 0 since only one
                        // t-minus value is acceptable. If we look to use
                        // the other t-minus data, this will need to change
                        cutoff = 0;

                        // Declare that this is analysis and assimilation
                        isAssim = true;
                    }

                    // If we are at or prior to the cut off point...
                    while (current <= cutoff)
                    {
                        // Get the initial template for the location of our
                        // data store
                        String addressTemplate = SystemSettings.getRemoteNetcdfURL();

                        // Create a new collection for our filenames
                        filenames = new ArrayList<>(  );

                        // If there is no http/ftp at the front of the address,
                        // add one (this is how the major browser handle it)
                        if (!(addressTemplate.toLowerCase().startsWith("http://") ||
                                addressTemplate.startsWith("https://") ||
                                addressTemplate.startsWith("ftp://")))
                        {
                            addressTemplate = "http://" + addressTemplate;
                        }

                        // If there is no forward slash to separate the location
                        // and the files, add it
                        if (!addressTemplate.endsWith("/"))
                        {
                            addressTemplate += "/";
                        }

                        // Build up the url for the date and range directory. It
                        // will be of the format:
                        //  "nwm/20170808/short_range/nwm.t"
                        addressTemplate += "nwm.";
                        addressTemplate += date;
                        addressTemplate += "/";
                        addressTemplate += range;
                        addressTemplate += "/";

                        // Begin the initial format for the filename
                        String filename = "nwm.t";

                        // For all start times for the forecasts in a day
                        // for the particular range...
                        for (int i = 0; i <= maxZTime; i += zIncrement)
                        {
                            // Add the number of the hour of the basis time
                            // to the file name and add it to the collection
                            // of names. Add padding to make sure it
                            // always has 2 digits
                            if (i < 10)
                            {
                                filenames.add( filename + "0" + String.valueOf(i));
                            }
                            else
                            {
                                filenames.add( filename + String.valueOf( i ) );
                            }
                        }

                        // The variable "filename" should not be used after
                        // this point

                        // Cap off the start time definitions with the z indicator,
                        // along with the separator for the following parts of the
                        // name
                        for (int i = 0; i < filenames.size(); ++i)
                        {
                            filenames.set( i, filenames.get(i) + "z.");
                        }

                        // If this is for long range data
                        if (isLong)
                        {
                            // Since the name for long range data can be of the form
                            // of "long_range_mem4" and not "long_range", we want
                            // to force the name "long_range" on all forecasts
                            // so that we can add the member number later
                            for (int i = 0; i < filenames.size(); ++i)
                            {
                                filenames.set( i, filenames.get(i) + "long_range");
                            }
                        }
                        else
                        {
                            // Otherwise, just add the name of the range to
                            // each file name
                            for (int i = 0; i < filenames.size(); ++i)
                            {
                                filenames.set( i, filenames.get(i) + range);
                            }
                        }

                        // If this is forcing data, the category should just be
                        // "forcing"
                        if (isForcing)
                        {
                            // Add forcing to the end of each filename
                            for (int i = 0; i < filenames.size(); ++i)
                            {
                                filenames.set( i, filenames.get(i) + ".forcing");
                            }

                            // Note: All categories for forcing data is
                            // contained within a single file
                        }
                        else
                        {
                            // Add the category to the end of each filename
                            for (int i = 0; i < filenames.size(); ++i)
                            {
                                filenames.set( i, filenames.get(i) + "." + category);
                            }
                        }

                        // Add the member ensemble identifier to the end
                        // of each file name if this is long range data
                        if (isLong)
                        {
                            // Get the member identifier by pulling it off of
                            // the range. If the range is "long_range_mem4",
                            // \\d$ will pull a number at the end of the word,
                            // i.e. 4
                            String memberTag = "_" + Strings.extractWord(range, "\\d$");

                            // Add the member identifier to each file name
                            for (int i = 0; i < filenames.size(); ++i)
                            {
                                filenames.set( i, filenames.get(i) + memberTag);
                            }
                        }

                        // Append the separator to the end of each file name
                        for (int i = 0; i < filenames.size(); ++i)
                        {
                            filenames.set( i, filenames.get(i) + ".");
                        }

                        // Add the lead time information for each file
                        if (isAssim)
                        {
                            // This will always default to t-minus 0.
                            // There should probably be a process for getting and
                            // storing t-minus 1 and t-minus 2 as well
                            for (int i = 0; i < filenames.size(); ++i)
                            {
                                filenames.set( i, filenames.get(i) + "tm00");
                            }
                        }
                        else
                        {
                            // The format for the lead information will be of the
                            // format of "f001" or "f240", so add the "f"
                            // and ensure that there is enough padding
                            String fTime = "f";
                            if (current < 100)
                            {
                                fTime += "0";
                                if (current < 10)
                                {
                                    fTime += "0";
                                }
                            }

                            // Add the "f", the padding, and the current
                            // lead time to the name of each file to download
                            for (int i = 0; i < filenames.size(); ++i)
                            {
                                filenames.set( i, filenames.get(i) + fTime + String.valueOf( current ));
                            }
                        }

                        // Add the extension to each file name
                        for (int i = 0; i < filenames.size(); ++i)
                        {
                            filenames.set( i, filenames.get(i) + ".conus.nc");

                            // If the requested information is prior to
                            // 2017-05-09, the water model was version 1.0,
                            // meaning that it was in a tar format
                            if (date.compareTo("20170509") < 0)
                            {
                                filenames.set( i, filenames.get(i) + ".gz");
                            }
                        }

                        // For each generated file name...
                        for ( String name : filenames)
                        {
                            // Create the download handler
                            final Downloader downloadOperation =
                                    new Downloader(Paths.get(downloadDirectory.getAbsolutePath(), name),
                                                   addressTemplate + name);

                            // Ensure that download messages are displayed
                            downloadOperation.setDisplayOutput( true );

                            // Store the name of the file to download with
                            // The future generated by sending the handler to
                            // the executor service
                            downloadOperations.put( filename,
                                                    Executor.execute(downloadOperation));
                        }

                        // Increment the lead hour and continue
                        current += hourIncrement;
                    }

                    // call ".get()" on each future to ensure that it finishes
                    // downloading
                    for (final Entry<String, Future> operation : downloadOperations.entrySet())
                    {
                        try
                        {
                            operation.getValue().get();
                        }
                        catch (InterruptedException | ExecutionException e)
                        {
                            LOGGER.error("An error was encountered while attempting to complete the download for '" + operation.getKey() + "'.");
                            throw e;
                        }
                    }

                    // Since no error was thrown, report a success
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
                // Since not enough information was passed, inform the user of
                // the require parameters
				LOGGER.warn("There are not enough parameters to download updated netcdf data.");
                LOGGER.warn("usage: downloadTestData <date> <range name> <category> [<cutoff hour>]");
                LOGGER.warn("Example: downloadTestData 20170508 long_range_mem4 land");
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
                LOGGER.warn("");
                LOGGER.warn("Acceptable categories are:");
                LOGGER.warn("   land");
                LOGGER.warn("   reservoir");
                LOGGER.warn("   channel_rt");
                LOGGER.warn("   terrain_rt");
			}

			return result;
		};
	}

	private static Function<String[], Integer> refreshDatabase ()
	{
		return (final String[] args) -> {
			Integer result = FAILURE;
			try {
                Operations.refreshDatabase();
                result = SUCCESS;
            }
            catch (final Exception e)
            {
                LOGGER.error(Strings.getStackTrace(e));
            }
            return result;
		};
	}

    private static Function<String[], Integer> ingest ()
    {
	    return (String[] args) -> {
	        int result = FAILURE;

	        if (args.length > 0)
            {
                PROJECT_PATH = args[0];

                ProjectConfig projectConfig;

                try
                {
                    projectConfig = ConfigHelper.read(PROJECT_PATH);
                    Operations.ingest(projectConfig);
                }
                catch ( IOException e )
                {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }
            else
            {
                LOGGER.error("There are not enough arguments to run 'ingest'");
                LOGGER.error("usage: ingest <path to configuration>");
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

					final String insertHeader = "INSERT INTO wres.NetCDFCoordinate (x_position, y_position, geographic_coordinate, resolution) VALUES ";
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
										   append(customSRID).append("), 4269)::point")
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

	private static Function<String[], Integer> loadFeatures() {
	    return (final String[] args) ->
        {
            Integer result = FAILURE;

            if (args.length > 0)
            {
                String filePath = args[0];
                try
                {

                    if (Files.notExists(Paths.get(filePath)) || ReaderFactory.getFiletype(filePath) != SourceType.NETCDF)
                    {
                        throw new IOException("There is not a NetCDFFile at the indicated path");
                    }

                    NetcdfFile file = NetcdfFile.open(filePath);

                    if (!NetCDF.hasVariable(file, "feature_id"))
                    {
                        throw new IOException("The NetCDF file at: '" + filePath + "' lacks a proper feature variable (feature_id)");
                    }

                    String getComidScript = "SELECT comid FROM wres.Feature WHERE comid != -999;";
                    Collection<Integer> comids = new HashSet<>();
                    comids = Database.populateCollection(comids, getComidScript, "comid");

                    Variable var = NetCDF.getVariable(file, "feature_id");
                    List<int[]> parameters = new ArrayList<>();;
                    Array features = var.read();

                    Function<List<int[]>, WRESRunnable> createThread = (List<int[]> params) ->
                    {
                        WRESRunnable runnable = new WRESRunnable() {
                            @Override
                            protected void execute () {
                                if (this.parameters == null || this.parameters.size() == 0)
                                {
                                    return;
                                }

                                Connection connection = null;
                                PreparedStatement statement = null;
                                LinkedList<Future<?>> updates = new LinkedList<>();
                                final String script = "UPDATE wres.Feature SET nwm_index = ? WHERE comid = ?;";
                                try
                                {
                                    connection = Database.getConnection();
                                    statement = connection.prepareStatement(script);

                                    for (int[] parameter : this.parameters)
                                    {
                                        statement.setInt(1, parameter[0]);
                                        statement.setInt(2, parameter[1]);
                                        statement.addBatch();
                                    }

                                    statement.executeBatch();
                                }
                                catch (SQLException e) {
                                    LOGGER.error(Strings.getStackTrace(e));
                                }
                                finally {
                                    if (statement != null)
                                    {
                                        try {
                                            statement.close();
                                        }
                                        catch (SQLException e) {
                                            LOGGER.error(Strings.getStackTrace(e));
                                        }
                                    }

                                    if (connection != null)
                                    {
                                        Database.returnConnection(connection);
                                    }
                                }
                            }

                            @Override
                            protected Logger getLogger () {
                                return MainFunctions.LOGGER;
                            }

                            public WRESRunnable init(List<int[]> parameters)
                            {
                                this.parameters = parameters;
                                return this;
                            }

                            private List<int[]> parameters;
                        }.init(params);

                        runnable.setOnRun(ProgressMonitor.onThreadStartHandler());
                        runnable.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                        return runnable;
                    };

                    for (Integer featureIndex = 0; featureIndex < features.getSize(); ++featureIndex)
                    {
                        if (parameters.size() >= SystemSettings.maximumDatabaseInsertStatements())
                        {
                            WRESRunnable runnable = createThread.apply(parameters);

                            Database.storeIngestTask(Database.execute(runnable));
                            parameters = new ArrayList<>();
                        }

                        if (comids.contains(features.getInt(featureIndex)))
                        {
                            parameters.add(new int[]{featureIndex, features.getInt(featureIndex)});
                        }
                    }

                    WRESRunnable runnable = createThread.apply(parameters);
                    Database.storeIngestTask(Database.execute(runnable));

                    Database.completeAllIngestTasks();

                }
                catch (IOException e) {
                    LOGGER.error(Strings.getStackTrace(e));
                }
                catch (SQLException e) {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }
            else
            {
                LOGGER.error("There are not enough arguments to run 'loadFeatures'");
                LOGGER.error("usage: loadCoordinates <Path to NetCDF file containing feature information>");
            }

            return result;
        };
    }

	private static Function<String[], Integer> install() {
		return (String[] args) -> {
			Operations.install();

			return FAILURE;
		};
	}
}
