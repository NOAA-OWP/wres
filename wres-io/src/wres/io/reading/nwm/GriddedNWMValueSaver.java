package wres.io.reading.nwm;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.io.concurrency.CopyExecutor;
import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.Internal;
import wres.util.NotImplementedException;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
class GriddedNWMValueSaver extends WRESRunnable
{
	private final static String DELIMITER = ",";
    private static final Logger LOGGER = LoggerFactory.getLogger(GriddedNWMValueSaver.class);

    private int sourceID = Integer.MIN_VALUE;
    private final int variableID;
    private final String fileName;
    private StringBuilder builder;
    private String tableDefinition;
    private int copyCount;
    private Variable variable;
    private NetcdfFile source;
    private Double invalidValue;
    private int xLength = Integer.MIN_VALUE;
    private int yLength = Integer.MIN_VALUE;
    private int rank = Integer.MIN_VALUE;
    private final Stack<Future<?>> operations = new Stack<>();
    private String hash;
    private final Future<String> futureHash;

    @Internal(exclusivePackage = "wres.io")
	public GriddedNWMValueSaver( String fileName, int variableID, Future<String> futureHash)
	{
		this.fileName = fileName;
		this.variableID = variableID;
		this.futureHash = futureHash;
	}

	@Override
    public void execute() {
        builder = new StringBuilder();
		try {

			Variable variable = getVariable();
			if (variable != null)
            {
                int[] origin = new int[getRank()];
                int[] size = new int[getRank()];
                size[0] = 1;

                int currentXIndex;
                int currentYIndex = 0;
                Integer yIndex;

                int half = getYLength()/2;
                int quarter = getYLength()/4;
                int threeQuarter = (getYLength()/4) * 3;

                while ((currentYIndex == 0 && getYLength() == 0) || currentYIndex < getYLength())
                {
                    ProgressMonitor.increment();

                    if (currentYIndex == half)
                    {
                        this.getLogger().trace("Currently halfway done with setting up jobs to save {} data from '{}'",
                                               this.variable.getShortName(),
                                               this.source.getLocation());
                    }
                    else if (currentYIndex == quarter)
                    {
                        this.getLogger().trace("Currently a quarter of the way done setting up jobs to save {} data from '{}'",
                                               this.variable.getShortName(),
                                               this.source.getLocation());
                    }
                    else if (currentYIndex == threeQuarter)
                    {
                        this.getLogger().trace("Currently three quarters of the way done setting up jobs to save {} data from '{}'",
                                               this.variable.getShortName(),
                                               this.source.getLocation());
                    }

                    currentXIndex = 0;

                    this.getLogger().trace("Now looping through a set of x values.");
                    for (; currentXIndex < getXLength(); ++currentXIndex)
                    {
                        ProgressMonitor.increment();

                        origin[1] = currentXIndex;
                        size[1] = Math.min(SystemSettings.getMaximumCopies(), getXLength() - currentXIndex);

                        if (this.getRank() == 3)
                        {
                            origin[2] = currentYIndex;
                            size[2] = 1;
                        }

                        Array data = variable.read(origin, size);

                        //Instead of running has next and get next, do a for(i) loop.

                        for (int xIndex = 0; xIndex < size[1]; ++xIndex)
                        {
                            if (getYLength() == 0)
                            {
                                yIndex = null;
                            }
                            else
                            {
                                yIndex = currentYIndex;
                            }
                            this.addLine(currentXIndex, yIndex, data.getDouble(xIndex));
                            currentXIndex++;
                        }

                        /*while (data.hasNext())
                        {
                            if (getYLength() == 0)
                            {
                                yIndex = null;
                            }
                            else
                            {
                                yIndex = currentYIndex;
                            }
                            this.addLine(currentXIndex, yIndex, data.nextDouble());
                            currentXIndex++;
                        }*/
                        ProgressMonitor.completeStep();
                    }
                    LOGGER.trace("Moving on to the next set of Y values");
                    currentYIndex++;
                    ProgressMonitor.completeStep();
                }

                copyValues();

                this.getLogger().trace("Now waiting for all tasks used to save {} from '{}' to finish...",
                                       this.variable.getShortName(),
                                       this.fileName);

                while (!this.operations.empty())
                {
                    try {
                        ProgressMonitor.increment();
                        this.operations.pop().get();
                        ProgressMonitor.completeStep();
                    }
                    catch (Exception e)
                    {
                        this.getLogger().error("Could not complete a task to save {} from '{}'",
                                               this.variable.getShortName(),
                                               this.fileName);
                    }
                }
            }
		} catch (Exception e) {
            LOGGER.error(Strings.getStackTrace(e));
		}
		finally
		{
            try {
                this.closeFile();
            } catch (IOException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }
	}

	private NetcdfFile getFile() throws IOException
    {
        if (this.source == null) {
            this.getLogger().trace("Now opening '{}'...", this.fileName);
            this.source = NetcdfFile.open(this.fileName);
            this.getLogger().trace("'{}' has been opened for parsing.", this.fileName);
        }
        return this.source;
    }

    private void closeFile() throws IOException
    {
        if (this.source != null)
        {
            this.source.close();
            this.source = null;
        }
    }

    private String getHash() throws ExecutionException, InterruptedException
    {
        if (this.hash == null)
        {
            if (this.futureHash != null)
            {
                this.hash = this.futureHash.get();
            }
            else
            {
                String message = "No hash operation was set during ingestion. ";
                message += "A hash for the source could not be determined.";
                LOGGER.error( message );
                throw new NotImplementedException( message );

            }
        }

        return this.hash;
    }

    private Variable getVariable() throws IOException {
        final String nameToFind = Variables.getName(this.variableID);

	    if (this.variable == null && Strings.hasValue(nameToFind))
        {
            NetcdfFile source = getFile();
            this.getLogger().trace("Now looking for {} inside '{}'", nameToFind, this.fileName);
            for (Variable var : source.getVariables())
            {
                if (var.getShortName().equalsIgnoreCase(nameToFind))
                {
                    this.variable = var;
                    break;
                }
            }
        }

        if (this.variable == null)
        {
            throw new IOException("The variable " +
                                          String.valueOf(nameToFind) +
                                          " could not be found within '" +
                                          this.fileName + "'");
        }
        
        return this.variable;
    }

	private void addLine(int xPosition, Integer yPosition, double value)
            throws IOException, SQLException, ExecutionException,
            InterruptedException
    {
		if (isValueValid(value))
		{
			if (builder == null)
			{
				builder = new StringBuilder();
			}

			builder.append(this.getSourceID());
			builder.append(DELIMITER);
			builder.append(this.variableID);
			builder.append(DELIMITER);
			builder.append(String.valueOf(xPosition));
			builder.append(DELIMITER);
			builder.append(String.valueOf(yPosition));
			builder.append(DELIMITER);
			builder.append(String.valueOf(value));
			builder.append(NEWLINE);

			this.copyCount++;

			if (this.copyCount >= SystemSettings.getMaximumCopies())
            {
                this.getLogger().trace("The copy count now exceeds the maximum allowable copies, so the values are being sent to save.");
                this.copyValues();
            }
		}
	}

	private void copyValues()
    {
        if (builder.length() > 0)
        {
            try {
                CopyExecutor copier = new CopyExecutor(getTableDefinition(), builder.toString(), DELIMITER);
                copier.setOnRun(ProgressMonitor.onThreadStartHandler());
                copier.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                this.getLogger().trace("Sending NetCDF values to the database executor to copy...");
                this.operations.push(Database.execute(copier));
            } catch (ExecutionException | InterruptedException | SQLException | IOException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
            builder = new StringBuilder();
            copyCount = 0;
        }
        else
        {
            this.getLogger().warn("Data is not being copied because the builder has no data.");
        }
    }

	private boolean isValueValid(double value)
	{
		return this.getInvalidValue() == Double.MIN_VALUE || !String.valueOf(value).equalsIgnoreCase(String.valueOf(this.getInvalidValue()));
	}

	private Double getInvalidValue()
    {
        if (this.invalidValue == null)
        {
            try {
                Attribute attr = this.getVariable().findAttribute("_FillValue");
                this.invalidValue = attr.getNumericValue().doubleValue();
            } catch (IOException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }
        return this.invalidValue;
    }

	private String getTableDefinition()
            throws ExecutionException, InterruptedException, IOException,
            SQLException
    {
		if (this.tableDefinition == null)
		{
		    String tableName = "NetCDFValue_Source_";
		    tableName += String.valueOf(this.getSourceID());
		    tableName += "_Variable_";
		    tableName += String.valueOf(variableID);

			String definitionScript;
			definitionScript = "CREATE TABLE IF NOT EXISTS partitions.";
			definitionScript += tableName;
			definitionScript += " (" + NEWLINE;
			definitionScript += "	CHECK ( source_id = " + this.getSourceID() + " AND variable_id = " + this.variableID + " )" + NEWLINE;
			definitionScript += ") INHERITS (wres.NetCDFValue);";

			Database.execute(new SQLExecutor(definitionScript)).get();

			Database.saveIndex("partitions." + tableName,
                               tableName + "_idx",
                               "(source_id, variable_id)");

			this.tableDefinition = "partitions.NETCDFVALUE_SOURCE_";
			this.tableDefinition += String.valueOf(this.getSourceID());
			this.tableDefinition += "_VARIABLE_";
			this.tableDefinition += String.valueOf(this.variableID);
			this.tableDefinition += " ( source_id, variable_id, x_position, y_position, variable_value)";

		}
		return this.tableDefinition;
	}

	private int getXLength() throws IOException {
        if (xLength == Integer.MIN_VALUE) {
            Variable var = getVariable();

            switch (var.getDimensions().size()) {
                case 1:
                    xLength = var.getDimension(0).getLength();
                    break;
                case 2:
                    xLength = var.getDimension(0).getLength();
                    break;
                case 3:
                    xLength = var.getDimension(1).getLength();
                    break;
            }
        }

        return this.xLength;
    }

    private int getYLength() throws IOException {

        if (this.yLength == Integer.MIN_VALUE) {
            Variable var = getVariable();
            switch (var.getDimensions().size()) {
                case 2:
                    yLength = var.getDimension(1).getLength();
                    break;
                case 3:
                    yLength = var.getDimension(2).getLength();
                    break;
            }
        }

        return this.yLength;
    }

    private int getRank()
    {
        if (this.rank == Integer.MIN_VALUE) {
            Variable var;
            try {
                var = this.getVariable();

                if (var != null) {
                    this.rank = var.getRank();
                }
            } catch (IOException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }
        return this.rank;
    }

    private int getSourceID()
            throws IOException, SQLException, ExecutionException,
            InterruptedException
    {
        if (this.sourceID == Integer.MIN_VALUE)
        {
            String leadDescription;
            Integer lead;
            String[] parts;

            String augmentedName = Paths.get(this.fileName).getFileName().toString();
            augmentedName = augmentedName.replaceAll("\\.gz", "");
            augmentedName = augmentedName.replaceAll("nwm\\.", "");
            augmentedName = augmentedName.replaceAll("\\.nc", "");

            parts = augmentedName.split("\\.");

            String range = Collections.find(parts, (String possibility) -> {
                return possibility.endsWith("range") || possibility.endsWith("assim");
            }).split("_")[0];

            if (range.equalsIgnoreCase("analysis")) {
                leadDescription = Collections.find(parts, (String possibility) -> {
                    return possibility.startsWith("t") || possibility.endsWith("z");
                });
            } else {
                leadDescription = Collections.find(parts, (String possibility) -> {
                    return possibility.startsWith("f");
                });
            }


            lead = Integer.parseInt(leadDescription.replaceAll("\\D", ""));

            Attribute attr = source.findGlobalAttributeIgnoreCase("model_initialization_time");
            String outputTime = attr.getStringValue().replaceAll("_", " ");

            if (range.equalsIgnoreCase("analysis")) {
                OffsetDateTime originalAssimTime = wres.util.Time.convertStringToDate(outputTime).minusHours(lead);
                outputTime = wres.util.Time.convertDateToString(originalAssimTime);
            }

            this.sourceID = DataSources.getSourceID(this.fileName,
                                                    outputTime,
                                                    lead,
                                                    this.getHash());
        }
        return this.sourceID;
    }

    @Override
    protected Logger getLogger () {
        return GriddedNWMValueSaver.LOGGER;
    }
}
