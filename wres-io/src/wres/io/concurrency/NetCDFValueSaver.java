package wres.io.concurrency;

import ucar.ma2.Array;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.ProgressMonitor;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
public class NetCDFValueSaver extends WRESThread implements Runnable
{
	private final static String DELIMITER = ",";

    private int sourceID = Integer.MIN_VALUE;
    private final String variableName;
    private final int variableID;
    private final String fileName;
    private StringBuilder builder;
    private String tableDefinition;
    private int copyCount;
    private Variable variable;
    private NetcdfFile source;
    private double invalidValue = Double.MIN_VALUE;
    private int xLength = Integer.MIN_VALUE;
    private int yLength = Integer.MIN_VALUE;
    private int rank = Integer.MIN_VALUE;
    private final Stack<Future<?>> operations = new Stack<>();

	public NetCDFValueSaver(String fileName, String variableName, int variableID, Double invalidValue)
	{
		this.fileName = fileName;
		this.variableName = variableName;
		this.variableID = variableID;
		this.invalidValue = invalidValue;
	}

	@Override
    public void run() {
	    this.executeOnRun();
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
                        System.err.println(NEWLINE +
                                "Currently halfway done with setting up jobs to save " +
                                this.variableName +
                                " data from '" +
                                this.source.getLocation() +
                                "'");
                    }
                    else if (currentYIndex == quarter)
                    {
                        System.err.println(NEWLINE +
                                "Currently a quarter of the way done setting up jobs to save " +
                                this.variableName +
                                " data from '" +
                                this.source.getLocation() +
                                "'");
                    }
                    else if (currentYIndex == threeQuarter)
                    {
                        System.err.println(NEWLINE +
                                "Currently three quarters of the way done setting up jobs to save " +
                                this.variableName +
                                " data from '" +
                                this.source.getLocation() +
                                "'");
                    }

                    currentXIndex = 0;

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

                    currentYIndex++;
                    ProgressMonitor.completeStep();
                }

                copyValues();

                System.out.println(NEWLINE + "Now waiting for all tasks used to save " + this.variableName + " from " + this.fileName + " to finish...");
                while (!this.operations.empty())
                {
                    try {
                        this.operations.pop().get();
                    }
                    catch (Exception e)
                    {
                        System.err.println(NEWLINE + "Could not complete a task to save " + this.variableName + " from " + this.fileName);
                    }
                }
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally
		{
            try {
                this.closeFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
		this.executeOnComplete();
	}

	private NetcdfFile getFile() throws IOException
    {
        if (this.source == null) {
            this.source = NetcdfFile.open(this.fileName);
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

    private Variable getVariable() throws IOException {
	    if (this.variable == null)
        {
            NetcdfFile source = getFile();

            for (Variable var : source.getVariables())
            {
                if (var.getShortName().equalsIgnoreCase(this.variableName))
                {
                    this.variable = var;
                    break;
                }
            }
        }
        
        return this.variable;
    }

	private void addLine(int xPosition, Integer yPosition, double value)
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
                this.operations.push(Database.execute(copier));
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }
            builder = new StringBuilder();
            copyCount = 0;
        }
        else
        {
            System.err.println("Data is not being copied because the builder has no data.");
        }
    }

	private boolean isValueValid(double value)
	{
		return this.getInvalidValue() == Double.MIN_VALUE || !String.valueOf(value).equalsIgnoreCase(String.valueOf(this.getInvalidValue()));
	}

	private Double getInvalidValue()
    {
        if (this.invalidValue == Double.MIN_VALUE)
        {
            try {
                Attribute attr = this.getVariable().findAttribute("_FillValue");
                this.invalidValue = attr.getNumericValue().doubleValue();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this.invalidValue;
    }

	private String getTableDefinition() throws ExecutionException, InterruptedException {
		if (this.tableDefinition == null)
		{
			String definitionScript;
			definitionScript = "CREATE TABLE IF NOT EXISTS partitions.NETCDFVALUE_SOURCE_";
			definitionScript += String.valueOf(this.getSourceID());
			definitionScript += "_VARIABLE_";
			definitionScript += String.valueOf(this.variableID);
			definitionScript += " (" + NEWLINE;
			definitionScript += "	CHECK ( source_id = " + this.getSourceID() + " AND variable_id = " + this.variableID + " )" + NEWLINE;
			definitionScript += ") INHERITS (wres.NetCDFValue);";

			Database.execute(new SQLExecutor(definitionScript)).get();
			this.tableDefinition = "wres.NETCDFVALUE_SOURCE_";
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
                e.printStackTrace();
            }
        }
        return this.rank;
    }

    private int getSourceID()
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

            try {
                this.sourceID = DataSources.getSourceID(this.fileName, outputTime, lead);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return this.sourceID;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.closeFile();
    }
}
