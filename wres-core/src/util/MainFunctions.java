package util;

import concurrency.Downloader;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.Control;
import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.io.concurrency.Executor;
import wres.io.concurrency.PairRetriever;
import wres.io.concurrency.SQLExecutor;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.Variables;
import wres.io.grouping.LabeledScript;
import wres.io.reading.SourceLoader;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptGenerator;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.XML;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
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
	private static final Map<String, Function<String[], Integer>> FUNCTIONS = createMap();

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
		prototypes.put("querynetcdf", queryNetCDF());
		prototypes.put("commands", printCommands());
		prototypes.put("--help", printCommands());
		prototypes.put("-h", printCommands());
		prototypes.put("flushdatabase", flushDatabase());
		prototypes.put("getpairs", getProjectPairs());
		prototypes.put("execute", new Control());
		prototypes.put("downloadtestdata", refreshTestData());
		prototypes.put("refreshstatistics", refreshStatistics());
		prototypes.put("loadcoordinates", loadCoordinates());
		prototypes.put("builddatabase", buildDatabase());
		prototypes.put("ingest", ingestByConfiguration());
		prototypes.put("testgzip", testGZIP());

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

                    while (results.next()) {
                        builder.append(results.getString(1)).append(NEWLINE);
                    }
                }
                catch (final SQLException e) {
                    LOGGER.error(Strings.getStackTrace(e));
                    throw e;
                }
                finally
                {
                    if (results != null)
                    {
                        results.close();
                    }

                    if (connection != null)
                    {
                        Database.returnConnection(connection);
                    }
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
	        if (args.length >= 1)
	        {
                try
                {
                    final String projectName = args[0];
                    LOGGER.info("The project name is: {}", projectName);
                    Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> pairMapping = new TreeMap<>();

                    final ProjectConfig foundProject = ConfigHelper.read(projectName);// ProjectSettings.getProject(projectName);
                    Integer variableId = Variables.getVariableID(foundProject
                                                                         .getInputs()
                                                                         .getRight()
                                                                         .getVariable()
                                                                         .getValue(),
                                                                 foundProject
                                                                         .getInputs()
                                                                         .getRight()
                                                                         .getVariable()
                                                                         .getUnit());

                    LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(variableId);

                    Integer finalLead = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());
                    Map<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>> threadResults = new TreeMap<>();

                    int step = 1;

                    while (ConfigHelper.leadIsValid(foundProject, step, finalLead))
                    {
                        PairRetriever pairRetriever = new PairRetriever(foundProject, step);
                        pairRetriever.setOnRun(ProgressMonitor.onThreadStartHandler());
                        pairRetriever.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                        threadResults.put(step, Database.submit(pairRetriever));
                        step++;
                    }

                    for (Entry<Integer, Future<List<PairOfDoubleAndVectorOfDoubles>>> threadResult : threadResults.entrySet())
                    {
                        pairMapping.put(threadResult.getKey(), threadResult.getValue().get());
                    }

                    final int printLimit = 100;
                    int printCount = 0;
                    final int totalLimit = 10;
                    int totalCount = 0;

                    LOGGER.info("");

                    for (final Integer leadKey : pairMapping.keySet())
                    {
                        LOGGER.info("\tLead Time: " + leadKey);
                        for (final PairOfDoubleAndVectorOfDoubles pair : pairMapping.get(leadKey))
                        {
                            final String representation = "\t\t" + pair.toString().substring(0, Math.min(120, pair.toString().length()));
                            LOGGER.info(representation);
                            
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

    private static Function<String[], Integer> ingestByConfiguration() {
	    return (String[] args) -> {
	        int result = FAILURE;

	        if (args.length > 0)
            {
                String configLocation = args[0];

                ProjectConfig projectConfig;

                try
                {
                    projectConfig = ConfigHelper.read(configLocation);
                    SourceLoader loader = new SourceLoader(projectConfig);
                    List<Future> ingestions = loader.load();

                    for (Future task : ingestions)
                    {
                        try {
                            task.get();
                        }
                        catch (InterruptedException | ExecutionException e) {
                            LOGGER.error(Strings.getStackTrace(e));
                        }
                    }

                    Future ingestTask = null;
                    try {
                        ingestTask = Database.getStoredIngestTask();

                        while (ingestTask != null)
                        {
                            try {
                                ingestTask.get();
                            }
                            catch (ExecutionException e) {
                                LOGGER.error(Strings.getStackTrace(e));
                            }
                            ingestTask = Database.getStoredIngestTask();
                        }
                    }
                    catch (InterruptedException e) {
                        LOGGER.error(Strings.getStackTrace(e));
                    }
                }
                catch (JAXBException | IOException e) {
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }
            else
            {
                LOGGER.error("There are not enough arguments to run 'ingestByConfiguration'");
                LOGGER.error("usage: ingestByConfiguration <path to configuration>");
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
			Database.buildInstance();

			return FAILURE;
		};
	}

	private static Function<String[], Integer> testGZIP() {
		return (String[] args) -> {

			final String fileName = "/home/ctubbs/workspace/wres/wres-core/testinput/sharedinput/example.tar.gz";

			final Path path = Paths.get(fileName);
			/*ZippedSource source = new ZippedSource(fileName);
			try {
				source.saveForecast();
			}
			catch (IOException e) {
				e.printStackTrace();
			}*/

			FileInputStream fileInputStream = null;
			BufferedInputStream bufferedInputStream = null;
			GzipCompressorInputStream decompressedFileStream = null;
			TarArchiveInputStream archiveInputStream = null;

			byte[] content;

			XMLStreamReader reader = null;

			try
			{
				fileInputStream = new FileInputStream(fileName);
				bufferedInputStream = new BufferedInputStream(fileInputStream);
				decompressedFileStream = new GzipCompressorInputStream(bufferedInputStream);
				archiveInputStream = new TarArchiveInputStream(decompressedFileStream);

                TarArchiveEntry entry = archiveInputStream.getNextTarEntry();

                XMLInputFactory factory = XMLInputFactory.newFactory();

                if (!entry.isFile())
                {
                    entry = archiveInputStream.getNextTarEntry();
                }

                while (entry != null)
                {
                    if (entry.isFile())
                    {
                        content = new byte[(int)entry.getSize()];
                        archiveInputStream.read(content, 0, content.length);

                        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(content);

                        reader = factory.createXMLStreamReader(byteArrayInputStream);

                        while (reader.hasNext())
                        {
                            reader.next();
                            if (XML.xmlTagClosed(reader, "TimeSeries"))
                            {
                                LOGGER.info("Hit the end of a forecast...");
                            }
                            else if (XML.tagIs(reader, "TimeSeries"))
                            {
                                LOGGER.info("Hit the start of a forecast...");
                                LOGGER.info("The name of the file is: " + path.toAbsolutePath().getParent() + "/" + entry.getName());
                            }
                        }
                    }

                    entry = archiveInputStream.getNextTarEntry();
                }

			}
			catch (XMLStreamException | IOException e) {
				e.printStackTrace();
			}
            finally
			{
				if (reader != null)
				{
					try {
						reader.close();
					}
					catch (XMLStreamException e) {
						e.printStackTrace();
					}
				}

				if (archiveInputStream != null)
				{
					try {
						archiveInputStream.close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (decompressedFileStream != null)
				{
					try {
						decompressedFileStream.close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (bufferedInputStream != null)
				{
					try {
						bufferedInputStream.close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (fileInputStream != null)
				{
					try {
						fileInputStream.close();
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

			return FAILURE;
		};
	}
}
