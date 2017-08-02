package util;

import concurrency.Downloader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.Control;
import wres.config.generated.Conditions;
import wres.config.generated.Coordinate;
import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.io.Operations;
import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.io.reading.ReaderFactory;
import wres.io.reading.SourceType;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptGenerator;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    // TODO: This is a dumb location/process for storing the path to a project. This needs to be refactored.
    private static String PROJECT_PATH = null;

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
                e.printStackTrace();
            }

        }

        return rawProject;
    }

	// Mapping of String names to corresponding methods
	private static final Map<String, Function<String[], Integer>> FUNCTIONS = createMap();

	public static void shutdown()
	{
	    ProgressMonitor.deactivate();
	    LOGGER.info("");
		LOGGER.info("Shutting down the application...");
		wres.io.Operations.shutdown();

        LOGGER.info("");
        LOGGER.info(Strings.getSystemStats());
	}

	/**
	 * Determines if there is a method for the requested operation
	 *
	 * @param operation The desired operation to perform
	 * @return True if there is a method mapped to the operation name
	 */
	public static boolean hasOperation (final String operation) {
		return FUNCTIONS.containsKey(operation.toLowerCase());
	}

	/**
	 * Executes the operation with the given list of arguments
	 *
	 * @param operation The name of the desired method to call
	 * @param args      The desired arguments to use when calling the method
	 */
	public static Integer call (String operation, final String[] args) {
		operation = operation.toLowerCase();
		final Integer result = FUNCTIONS.get(operation).apply(args);
		return result;
	}

	/**
	 * Creates the mapping of operation names to their corresponding methods
	 */
	private static Map<String, Function<String[], Integer>> createMap () {
		final Map<String, Function<String[], Integer>> prototypes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		prototypes.put("describenetcdf", describeNetCDF());
		prototypes.put("connecttodb", connectToDB());
		prototypes.put("querynetcdf", queryNetCDF());
		prototypes.put("commands", printCommands());
		prototypes.put("--help", printCommands());
		prototypes.put("-h", printCommands());
		prototypes.put("cleandatabase", cleanDatabase());
		prototypes.put("getpairs", getPairs());
		prototypes.put("execute", new Control());
		prototypes.put("downloadtestdata", refreshTestData());
		prototypes.put("refreshdatabase", refreshDatabase());
		prototypes.put("loadcoordinates", loadCoordinates());
		prototypes.put("install", install());
		prototypes.put("ingest", ingest());
		prototypes.put("generatepairscript", generatePairScript());
		prototypes.put("loadfeatures", loadFeatures());

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
                    reader.printQuery(variable_name, variable_args);
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

	private static Function<String[], Integer> refreshTestData () {
		return (final String[] args) -> {
			Integer result = FAILURE;
			if (args.length >= 2) {
			    final ExecutorService executorService = Executors.newFixedThreadPool(SystemSettings.maximumThreadCount());
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
                        downloadOperations.put(filename, executorService.submit(downloadOperation));

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

	private static Function<String[], Integer> getPairs()
	{
	    return (final String[] args) -> {
			Integer result = FAILURE;
	        if (args.length >= 1)
	        {
                try
                {
                    PROJECT_PATH = args[0];
                    LOGGER.info("The project is from: {}", PROJECT_PATH);

                    final ProjectConfig foundProject = ProjectConfigPlus.from(Paths.get(PROJECT_PATH)).getProjectConfig();

                    final Map<String, Map<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>>> pairMapping = new TreeMap<>();

                    for (Conditions.Feature feature : foundProject.getConditions().getFeature())
                    {
                        String description = feature.toString();

                        if (feature.getLocation() != null)
                        {
                            description = feature.getLocation().getLid();
                        }
                        else if (feature.getPoint() != null)
                        {
                            description = "(" + String.valueOf(feature.getPoint().getX()) + "," +
                                    String.valueOf(feature.getPoint().getY()) + ")";
                        }
                        else if (feature.getPolygon() != null)
                        {
                            description = "[";

                            for (Coordinate point : feature.getPolygon().getPoint())
                            {
                                description += "(" + String.valueOf(point.getX()) + "," +
                                        String.valueOf(point.getY()) + ")";
                            }

                            description += "]";
                        }
                        else if (feature.getIndex() != null)
                        {
                            description = "X = " + String.valueOf(feature.getIndex().getX()) +
                                    ", Y = " + String.valueOf(feature.getIndex().getY());
                        }

                        pairMapping.put(description, Operations.getPairs(foundProject, feature));
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
	            LOGGER.error("There are not enough arguments to run 'getProjectPairs'");
	            LOGGER.error("usage: getProjectPairs <project name> <metric name>");
	        }
	        return result;
	    };
	}

	private static Function<String[], Integer> generatePairScript()
    {
        return (String[] args) ->
        {
            int result = SUCCESS;

            if (args.length > 0)
            {
                PROJECT_PATH = args[0];

                try {
                    ProjectConfig config = ConfigHelper.read(PROJECT_PATH);

                    Conditions.Feature firstFeature = config.getConditions().getFeature().get(0);

                    String script = ScriptGenerator.generateGetPairData(config, firstFeature, 1);

                    LOGGER.info(script);
                }
                catch (Exception error) {
                    LOGGER.error(Strings.getStackTrace(error));
                    result = FAILURE;
                }
            }
            else
            {
                LOGGER.error("Not enough parameters were entered to generate a pair script");
                LOGGER.info("usage: generatePairScript <path to project>");
                result = FAILURE;
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
                catch (JAXBException | IOException e) {
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

                    Variable var = NetCDF.getVariable(file, "feature_id");
                    List<int[]> parameters = new ArrayList<>();;
                    Array features = var.read();

                    Function<List<int[]>, WRESRunnable> createThread = (List<int[]> params) ->
                    {
                        return new WRESRunnable() {
                            @Override
                            protected void execute () {
                                if (this.parameters == null || this.parameters.size() == 0)
                                {
                                    return;
                                }

                                Connection connection = null;
                                CallableStatement statement = null;
                                try
                                {
                                    connection = Database.getConnection();
                                    statement = connection.prepareCall("{call wres.add_netcdffeature(?, ?)}");

                                    for (int[] parameter : this.parameters)
                                    {
                                        statement.setInt(1, parameter[0]);
                                        statement.setInt(2, parameter[1]);
                                        statement.addBatch();
                                    }
                                    statement.executeBatch();
                                }
                                catch (SQLException e) {
                                    e.printStackTrace();
                                }
                                finally {
                                    if (statement != null)
                                    {
                                        try {
                                            statement.close();
                                        }
                                        catch (SQLException e) {
                                            e.printStackTrace();
                                        }
                                    }

                                    if (connection != null)
                                    {
                                        Database.returnConnection(connection);
                                    }
                                }
                            }

                            @Override
                            protected String getTaskName () {
                                return "Adding Features " +
                                        String.valueOf(this.parameters.get(0)[1]) +
                                        " through " +
                                        String.valueOf(this.parameters.get(this.parameters.size() - 1)[1]);
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
                    };

                    for (Integer featureIndex = 0; featureIndex < features.getSize(); ++featureIndex)
                    {
                        if (parameters.size() >= SystemSettings.maximumDatabaseInsertStatements())
                        {
                            WRESRunnable runnable = createThread.apply(parameters);

                            Database.storeIngestTask(Database.execute(runnable));
                            parameters = new ArrayList<>();
                        }

                        parameters.add(new int[]{featureIndex, features.getInt(featureIndex)});
                    }

                    WRESRunnable runnable = createThread.apply(parameters);
                    Database.storeIngestTask(Database.execute(runnable));

                    Database.completeAllIngestTasks();

                }
                catch (IOException e) {
                    e.printStackTrace();
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
