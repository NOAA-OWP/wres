package wres;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.unidata.geoloc.LatLonPoint;

import wres.config.ProjectConfigPlus;
import wres.config.Validation;
import wres.config.generated.ProjectConfig;
import wres.control.Control;
import wres.io.Operations;
import wres.io.concurrency.CopyExecutor;
import wres.io.concurrency.Downloader;
import wres.io.concurrency.Executor;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.reading.usgs.USGSParameterReader;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.NetCDF;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
final class MainFunctions
{
	static final Integer FAILURE = -1;
	static final Integer SUCCESS = 0;
    private static final Logger LOGGER = LoggerFactory.getLogger(MainFunctions.class);

	// Mapping of String names to corresponding methods
	private static final Map<String, Function<String[], Integer>> FUNCTIONS = createMap();

    static void shutdown()
	{
	    ProgressMonitor.deactivate();
	    LOGGER.info("");
		LOGGER.info("Shutting down the application...");
		wres.io.Operations.shutdown();
	}

    static void forceShutdown( long timeOut, TimeUnit timeUnit )
    {
        ProgressMonitor.deactivate();
        LOGGER.info("");
        LOGGER.info( "Forcefully shutting down the application (you may see some errors)..." );
        wres.io.Operations.forceShutdown( timeOut, timeUnit );
    }

	/**
	 * Determines if there is a method for the requested operation
	 *
	 * @param operation The desired operation to perform
	 * @return True if there is a method mapped to the operation name
	 */
	static boolean hasOperation (final String operation)
    {
		return FUNCTIONS.containsKey(operation.toLowerCase());
	}

	/**
	 * Executes the operation with the given list of arguments
	 *
	 * @param operation The name of the desired method to call
	 * @param args      The desired arguments to use when calling the method
	 */
	static Integer call (String operation, final String[] args) {
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
		final Map<String, Function<String[], Integer>> functions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		functions.put("connecttodb", connectToDB());
		functions.put("commands", MainFunctions::printCommands);
		functions.put("--help", MainFunctions::printCommands);
		functions.put("-h", MainFunctions::printCommands);
		functions.put("cleandatabase", cleanDatabase());
		functions.put("execute", new Control());
		functions.put("downloadtestdata", refreshTestData());
		functions.put("refreshdatabase", refreshDatabase());
		functions.put("loadcoordinates", loadCoordinates());
		functions.put("ingest", MainFunctions::ingest);
		functions.put( "loadusgsparameters", MainFunctions::loadUSGSParameters);
		functions.put("createnetcdftemplate", MainFunctions::createNetCDFTemplate);
		functions.put("validate", MainFunctions::validate);
		functions.put("validategrid", MainFunctions::validateNetcdfGrid);
		functions.put("readheader", MainFunctions::readHeader);

		return functions;
	}

	private static Integer loadUSGSParameters(final String[] args)
    {
        Integer result = FAILURE;

        if (args.length >= 1)
        {
            USGSParameterReader reader = new USGSParameterReader( args[0] );
            try
            {
                reader.read();
                result = SUCCESS;
            }
            catch ( IOException e )
            {
                MainFunctions.addException( e );
                LOGGER.error( Strings.getStackTrace( e ) );
            }
        }
        else
        {
            LOGGER.error("The path to the USGS parameter definition CSV is required.");
            LOGGER.error("usage: loadUSGSParameters parameters.csv");
        }

        return result;
    }

	/**
	 * Creates the "print_commands" method
	 *
	 * @return Method that prints all available commands by name
	 */
	private static Integer printCommands(final String[] args)
	{
		LOGGER.info("Available commands are:");
        for (final String command : FUNCTIONS.keySet())
        {
            LOGGER.info("\t{}", command);
        }
        return SUCCESS;
	}

	/**
	 * Creates the "connectToDB" method
	 *
	 * @return method that will attempt to connect to the database to prove that a connection is possible. The version of the connected database will be printed.
	 */
	private static Function<String[], Integer> connectToDB () {
		return (final String[] args) -> {
		    try
            {
                Operations.testConnection();
                return SUCCESS;
            }
            catch ( SQLException se )
            {
                LOGGER.warn( "Could not connect to database.", se );
                return FAILURE;
            }
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
            Integer result;
            try
            {
                Operations.cleanDatabase();
                result = SUCCESS;
            }
            catch ( IOException | SQLException e )
            {
                LOGGER.error( "While cleaning the database", e );
                result = FAILURE;
            }
			return result;
		};
	}

	private static Integer validateNetcdfGrid(String[] args)
    {
        Integer result = FAILURE;

        String path = args[0];
        String variableName = args[1];

        try (NetcdfDataset dataset = NetcdfDataset.openDataset( path ); GridDataset grid = new GridDataset( dataset ))
        {
            GridDatatype variable = grid.findGridDatatype( variableName );

            if (variable == null)
            {
                LOGGER.error("The given variable is not a valid projected grid variable.");
            }
            else
            {
                GridCoordSystem coordSystem = variable.getCoordinateSystem();

                if ( coordSystem != null )
                {
                    result = SUCCESS;
                }
            }
        }
        catch ( IOException e )
        {
            LOGGER.error("The file at {} is not a valid Netcdf Grid Dataset.", path);
        }

        return result;
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
				String range = args[1];

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
                            MainFunctions.addException( exception );
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
                                filenames.set( i, filenames.get(i).replace("forcing_", "") + ".forcing");
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
                            MainFunctions.addException( e );
                            LOGGER.error("An error was encountered while attempting to complete the download for '" + operation.getKey() + "'.");
                            throw e;
                        }
                    }

                    // Since no error was thrown, report a success
                    result = SUCCESS;
                }
                catch (final Exception e)
                {
                    MainFunctions.addException( e );
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
                MainFunctions.addException( e );
                LOGGER.error(Strings.getStackTrace(e));
            }
            return result;
		};
	}

    private static Integer ingest(String[] args)
    {
        int result = FAILURE;

        if (args.length > 0)
        {
            String projectPath = args[0];

            ProjectConfig projectConfig;

            try
            {
                projectConfig = ConfigHelper.read(projectPath);
                Operations.ingest(projectConfig);
                result = SUCCESS;
            }
            catch ( IOException e )
            {
                MainFunctions.addException( e );
                LOGGER.error(Strings.getStackTrace(e));
            }
        }
        else
        {
            LOGGER.error("There are not enough arguments to run 'ingest'");
            LOGGER.error("usage: ingest <path to configuration>");
        }

        return result;
    }

    private static Integer validate(String[] args)
    {
        int result = FAILURE;

        if (args.length > 0)
        {
            Path configPath = Paths.get( args[0] );
            ProjectConfigPlus projectConfigPlus;

            String fullPath = configPath.toAbsolutePath().toString();

            try
            {
                // Unmarshal the configuration
                projectConfigPlus = ProjectConfigPlus.from( configPath );
            }
            catch ( IOException ioe )
            {
                LOGGER.error("Failed to unmarshal the project configuration at '" + fullPath + "'");
                LOGGER.error(Strings.getStackTrace( ioe ));
                return result; // Or return 400 - Bad Request (see #41467)
            }

            // Validate unmarshalled configurations
            final boolean validated =
                    Validation.isProjectValid( projectConfigPlus );

            if ( validated )
            {
                LOGGER.info("'" + fullPath + "' is a valid project config.");
                result = SUCCESS;
            }
            else
            {
                // Even though the application performed its job, we still want
                // to return a failure so that the return code may be used to
                // determine the validity
                LOGGER.info("'" + fullPath + "' is not a valid config.");
            }

        }
        else
        {
            LOGGER.error( "A project path was not passed in" );
            LOGGER.error("usage: validate <path to project>");
        }

        return result;
    }

    private static Integer createNetCDFTemplate(String[] args)
    {
        int result = FAILURE;

        if (args.length > 1)
        {
            try
            {
                String fromFileName = args[0];
                String toFileName = args[1];

                if (!Files.exists( Paths.get( fromFileName ) ))
                {
                    throw new IllegalArgumentException( "The source file '" +
                                                        fromFileName +
                                                        "' does not exist. A "
                                                        + "template file must "
                                                        + "have a valid source "
                                                        + "file." );
                }

                if (!toFileName.toLowerCase().endsWith( "nc" ))
                {
                    throw new IllegalArgumentException( "The name for the "
                                                        + "template is invalid; "
                                                        + "it must end with "
                                                        + "'*.nc' to indicate "
                                                        + "that it is a NetCDF "
                                                        + "file." );
                }

                Operations.createNetCDFOutputTemplate( fromFileName, toFileName );
                result = SUCCESS;
            }
            catch (IOException | RuntimeException error)
            {
                MainFunctions.addException( error );
                LOGGER.error(Strings.getStackTrace( error ));
            }
        }
        else
        {
            LOGGER.error("There are not enough arguments to create a template.");
            LOGGER.error("usage: createnetcdftemplate <path to original file> <path to template>");
        }

        return result;
    }

    private static Function<String[], Integer> loadCoordinates() {
		return (final String[] args) -> {
			Integer result = FAILURE;

			if (args.length > 0) {
			    NetcdfFile file = null;

				try
				{
                    // Assign path to NetCDF file
                    String filePath = args[0];

                    if (Files.notExists(Paths.get(filePath)))
                    {
                        throw new IOException("There is not a NetCDF file at the indicated path.");
                    }

					file = NetcdfFile.open(args[0]);

                    // Make sure that NetCDF file has the required coordinate variables (can't rely on the isCoordinate attribute)
					if (!NetCDF.hasVariable(file, "x") || !NetCDF.hasVariable(file, "y"))
					{
						throw new IOException("The NetCDF file at: '" + args[0] + "' lacks the proper X and Y coodinates.");
					}

                    // Loop through a full join across all contained x and y values
                    Variable xCoordinates = NetCDF.getVariable(file, "x");
                    Variable yCoordinates = NetCDF.getVariable(file, "y");
                    Variable coordinateSystem = NetCDF.getVariable( file, "ProjectionCoordinateSystem" );

                    String srtext = coordinateSystem.findAttValueIgnoreCase( "esri_pe_string", "" );
                    String proj4 = coordinateSystem.findAttValueIgnoreCase( "proj4", "" );
                    String projectionMapping = coordinateSystem.findAttValueIgnoreCase( "grid_mapping_name", "lambert_conformal_conic" );


                    Number xResolution = xCoordinates.findAttribute( "resolution" ).getNumericValue();
                    Number yResolution = yCoordinates.findAttribute("resolution").getNumericValue();
                    long xSize = xCoordinates.getSize();
                    long ySize = yCoordinates.getSize();
                    String xUnit = xCoordinates.findAttValueIgnoreCase( "units", "" );
                    String yUnit = yCoordinates.findAttValueIgnoreCase( "units", "" );
                    String xType = xCoordinates.findAttValueIgnoreCase( "_CoordinateAxisType", "GeoX" );
                    String yType = yCoordinates.findAttValueIgnoreCase( "_CoordinateAxisType", "GeoY" );

                    DataScripter script = new DataScripter(  );
                    script.addLine("WITH new_projection AS");
                    script.addLine("(");
                    script.addTab().addLine("INSERT INTO wres.GridProjection (");
                    script.addTab(  2  ).addLine("srtext,");
                    script.addTab(  2  ).addLine("proj4,");
                    script.addTab(  2  ).addLine("projection_mapping,");
                    script.addTab(  2  ).addLine("x_resolution,");
                    script.addTab(  2  ).addLine("y_resolution,");
                    script.addTab(  2  ).addLine("x_unit,");
                    script.addTab(  2  ).addLine("y_unit,");
                    script.addTab(  2  ).addLine("x_type,");
                    script.addTab(  2  ).addLine("y_type,");
                    script.addTab(  2  ).addLine("x_size,");
                    script.addTab(  2  ).addLine("y_size");
                    script.addTab().addLine(")");
                    script.addTab().addLine("SELECT '", srtext, "',");
                    script.addTab(  2  ).addLine("'", proj4, "',");
                    script.addTab(  2  ).addLine("'", projectionMapping, "',");
                    script.addTab(  2  ).addLine(xResolution, ",");
                    script.addTab(  2  ).addLine(yResolution, ",");
                    script.addTab(  2  ).addLine("'", xUnit, "',");
                    script.addTab(  2  ).addLine("'", yUnit, "',");
                    script.addTab(  2  ).addLine("'", xType, "',");
                    script.addTab(  2  ).addLine("'", yType, "',");
                    script.addTab(  2  ).addLine(xSize, ",");
                    script.addTab(  2  ).addLine(ySize);
                    script.addTab().addLine("WHERE NOT EXISTS (");
                    script.addTab(  2  ).addLine("SELECT 1");
                    script.addTab(  2  ).addLine("FROM wres.GridProjection");
                    script.addTab(  2  ).addLine("WHERE srtext = '", srtext, "'");
                    script.addTab(   3   ).addLine("AND proj4 = '", proj4, "'");
                    script.addTab(   3   ).addLine("AND x_resolution = ", xResolution);
                    script.addTab(   3   ).addLine("AND y_resolution = ", yResolution);
                    script.addTab(   3   ).addLine("AND x_unit = '", xUnit, "'");
                    script.addTab(   3   ).addLine("AND y_unit = '", yUnit, "'");
                    script.addTab(   3   ).addLine("AND x_type = '", xType, "'");
                    script.addTab(   3   ).addLine("AND y_type = '", yType, "'");
                    script.addTab(   3   ).addLine("AND x_size = ", xSize);
                    script.addTab(   3   ).addLine("AND y_size = ", ySize);
                    script.addTab().addLine(")");
                    script.addTab().addLine("RETURNING gridprojection_id");
                    script.addLine(")");
                    script.addLine("SELECT gridprojection_id, false AS load_complete");
                    script.addLine("FROM new_projection");
                    script.addLine();
                    script.addLine("UNION");
                    script.addLine();
                    script.addLine("SELECT gridprojection_id, load_complete");
                    script.addLine("FROM wres.GridProjection");
                    script.addLine("WHERE srtext = '", srtext, "'");
                    script.addTab(  2  ).addLine("AND proj4 = '", proj4, "'");
                    script.addTab(  2  ).addLine("AND x_resolution = ", xResolution);
                    script.addTab(  2  ).addLine("AND y_resolution = ", yResolution);
                    script.addTab(  2  ).addLine("AND x_unit = '", xUnit, "'");
                    script.addTab(  2  ).addLine("AND y_unit = '", yUnit, "'");
                    script.addTab(  2  ).addLine("AND x_type = '", xType, "'");
                    script.addTab(  2  ).addLine("AND y_type = '", yType, "'");
                    script.addTab(  2  ).addLine("AND x_size = ", xSize);
                    script.addTab(  2  ).addLine("AND y_size = ", ySize, ";");

                    List<Pair<Integer, Boolean>> projection = script.interpret(
                            scriptResult -> Pair.of(
                                    scriptResult.getInt( "gridprojection_id" ),
                                    scriptResult.getBoolean( "load_complete" )
                            )
                    );

                    Integer gridProjectionID = projection.get(0).getKey();
                    Boolean projectionAlreadyLoaded = projection.get(0).getValue();

                    if (projectionAlreadyLoaded)
                    {
                        LOGGER.info("The requested projection has already been loaded.");
                        return SUCCESS;
                    }

                    LOGGER.info("Removing any preexisting data that may have been present for this projection.");
                    script = new DataScripter(  );
                    script.addLine("DELETE FROM wres.NetCDFCoordinate");
                    script.addLine("WHERE gridprojection_id = ", gridProjectionID, ";");
                    script.execute();

                    final String COPY_HEADER = "wres.NetCDFCoordinate (gridprojection_id, x_position, y_position, x, y, geographic_coordinate)";
                    final String DELIMITER = "|";

					GridDataset grid = new GridDataset( new NetcdfDataset( file ) );
					GridCoordSystem coordSystem = grid.getGrids().get( 0 ).getCoordinateSystem();

                    StringJoiner copyValues = new StringJoiner(System.lineSeparator());
                    Queue<Future> copyTasks = new LinkedList<>(  );
                    int copyCount = 0;

                    ProgressMonitor.setShowStepDescription( true );

					for (int xIndex = 0; xIndex < xSize; ++xIndex)
                    {
                        for (int yIndex = 0; yIndex < ySize; ++yIndex)
                        {
                            StringJoiner line = new StringJoiner(DELIMITER);
                            LatLonPoint point = coordSystem.getLatLon( xIndex, yIndex);
                            line.add( String.valueOf(gridProjectionID) );
                            line.add(String.valueOf(xIndex));
                            line.add(String.valueOf(yIndex));
                            line.add(String.valueOf(xCoordinates.read(new int[]{xIndex}, new int[]{1})));
                            line.add(String.valueOf(yCoordinates.read( new int[]{yIndex}, new int[]{1} )));
                            line.add("(" + point.getLongitude() + "," + point.getLatitude() + ")");
                            copyValues.add( line.toString() );
                            copyCount++;

                            if (copyCount >= SystemSettings.getMaximumCopies())
                            {
                                WRESRunnable copier = new CopyExecutor( COPY_HEADER, copyValues.toString(), DELIMITER );
                                copier.setOnRun( ProgressMonitor.onThreadStartHandler() );
                                copier.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
                                copyTasks.add( Database.execute( copier ) );
                                copyValues = new StringJoiner( System.lineSeparator() );
                                copyCount = 0;
                            }

                        }
                    }

                    if (copyCount > 0)
                    {
                        ProgressMonitor.increment();
                        WRESRunnable copier = new CopyExecutor( COPY_HEADER, copyValues.toString(), DELIMITER );
                        copier.setOnRun( ProgressMonitor.onThreadStartHandler() );
                        copier.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
                        copyTasks.add( Database.execute( copier ) );
                    }

                    Future<?> copyTask = copyTasks.poll();

					while( Objects.nonNull(copyTask))
                    {
                        try
                        {
                            copyTask.get( 500, TimeUnit.MILLISECONDS );
                        }
                        catch(TimeoutException e)
                        {
                            LOGGER.trace("It took too long to copy a set of "
                                         + "values; moving on to the next set "
                                         + "of values.");
                            copyTasks.add( copyTask );
                        }

                        copyTask = copyTasks.poll();
                    }

                    script = new DataScripter(  );
					script.addLine("UPDATE wres.GridProjection");
					script.addTab().addLine("SET load_complete = true");
					script.addLine("WHERE gridprojection_id = ", gridProjectionID, ";");
					script.execute();

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
                    MainFunctions.addException( e );
                    LOGGER.error(Strings.getStackTrace(e));
                    if (file != null)
                    {
                        try
                        {
                            file.close();
                        }
                        catch ( IOException e1 )
                        {
                            MainFunctions.addException( e1 );
                        }
                    }
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

    private static Integer readHeader(String[] args)
    {
        Integer result = FAILURE;
        final String url = "http://***REMOVED***rgw.***REMOVED***.***REMOVED***:8080/nwm/nwm.20180515/analysis_assim/nwm.t00z.analysis_assim.channel_rt.tm00.conus.nc";

        // TODO: Finish experiment; current work must be halted

        /*
         * The idea here is that we want to see if we can pull the top x
         * bytes from the remote file. From there, we can pull and work with
         * just the header of a netcdf file.  If the data within the file
         * meets expectations, we may continue to work with it. If not,
         * we can avoid further work. If we can't avoid it, we are forced
         * to download a full file before we can determine whether or not
         * it can/should be used.
         */

        // Create a connection and open a stream to the resource
        URL sourcePath = null;
        try
        {
            sourcePath = new URL( url);
        }
        catch ( MalformedURLException e )
        {
            LOGGER.error("The url pointing towards the file was incorrect.", e);
            return FAILURE;
        }

        try(InputStream stream = sourcePath.openStream())
        {
            // The idea of 200 bytes for the header was pushed.
            // Working with 300 for initial tests just to be safe
            byte[] buffer = new byte[300];

            // Fill the buffer and attempt to load it into a Netcdf object
            // This is supposed to work in Thredds; it's called
            // "Range Subsetting"
            int bytesRead = stream.read( buffer );
            try (NetcdfFile data = NetcdfFile.openInMemory( "file", buffer ))
            {
                LOGGER.info( "Loaded data..." );
            }

            // If/when we can load the header, we want to be able to
            // evaluate the contents of it without actually hitting the
            // data. This might mean that attributes storing single
            // value data also contained within variables might
            // see use again. While "time" and "reference_time" are
            // the canonical sources of time and issue time data,
            // We most likely won't be able to hit that data
            // with the header alone (or at least reliably).
            result = SUCCESS;
        }
        catch ( IOException e )
        {
            LOGGER.error("Remote Netcdf reading failed.", e);
        }

        return result;
    }

    private static final Object EXCEPTION_LOCK = new Object();
    private static List<Exception> encounteredExceptions;

    private static void addException(Exception recentException)
    {
        synchronized ( EXCEPTION_LOCK )
        {
            if (MainFunctions.encounteredExceptions == null)
            {
                MainFunctions.encounteredExceptions = new ArrayList<>(  );
            }

            MainFunctions.encounteredExceptions.add(recentException);
        }
    }

    static List<Exception> getEncounteredExceptions()
    {
        synchronized ( EXCEPTION_LOCK )
        {
            if (MainFunctions.encounteredExceptions == null)
            {
                MainFunctions.encounteredExceptions = new ArrayList<>(  );
            }

            MainFunctions.encounteredExceptions.addAll( Control.getMostRecentException() );

            return Collections.unmodifiableList(MainFunctions.encounteredExceptions);
        }
    }
}
