package wres.io.reading.ucar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.io.concurrency.Executor;
import wres.io.concurrency.NetCDFValueSaver;
import wres.io.concurrency.WRESTask;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.ForecastDetails;
import wres.io.data.details.VariableDetails;
import wres.io.reading.BasicSource;
import wres.util.Collections;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author ctubbs
 *
 */
public class NetCDFSource extends BasicSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetCDFSource.class);

	private class VariableInserter extends WRESTask implements Runnable
	{

		public VariableInserter(Variable variableToInsert)
		{
			this.unitName = variableToInsert.getUnitsString();
			this.variableName = variableToInsert.getShortName();
			this.xLength = getXLength(variableToInsert);
			this.yLength = getYLength(variableToInsert);
		}

		@Override
		public void run() {
		    this.executeOnRun();

			//try {
                ProgressMonitor.increment();

            try {

                VariableDetails details = new VariableDetails();
                details.setVariableName(this.variableName);
                details.measurementunitId = MeasurementUnits.getMeasurementUnitID(this.unitName);
				LOGGER.trace("Getting the id for the variable '{}'", this.variableName);
                //Integer variableId = Variables.addVariable(details, this.xLength, this.yLength);
                Integer variableId = Variables.getVariableID(details);

                NetCDFValueSaver saver = new NetCDFValueSaver(getFilename(),
                                                              this.variableName,
                                                              variableId,
                                                              getMissingValue());
                saver.setOnRun(ProgressMonitor.onThreadStartHandler());
                saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                LOGGER.trace("Telling the general executor to save '{}' from '{}'", this.variableName, getFilename());
                //Executor.execute(saver);
                saver.run();

                ProgressMonitor.completeStep();
            }
            catch (SQLException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
			this.executeOnComplete();
		}

		private final int xLength;
		private final int yLength;
		private final String variableName;
		private final String unitName;

		@Override
		protected String getTaskName () {
			return "NetCDFSource.VariableSaver: " + this.variableName;
		}
	}
	/**
	 * 
	 */
	public NetCDFSource(String filename) {
		this.set_filename(filename);
	}
	
	@Override
	public void saveObservation() throws IOException
	{
		
	}
	
	@Override
	public void saveForecast() throws IOException
	{
		NetcdfFile source = getSource();
		Attribute attr = source.findGlobalAttributeIgnoreCase("model_initialization_time");
        this.modelInitializationTime = attr.getStringValue().replaceAll("_", " ");

        if (this.getRange().equalsIgnoreCase("analysis"))
        {
            OffsetDateTime originalAssimTime = wres.util.Time.convertStringToDate(this.modelInitializationTime).minusHours(this.getLead());
            this.modelInitializationTime = wres.util.Time.convertDateToString(originalAssimTime);
        }

		attr = source.findGlobalAttributeIgnoreCase("missing_value");
		if (attr != null)
		{
			this.missingValue = attr.getNumericValue().doubleValue();
		}

		//this.getForecastID();
		saveVariables(source);
		//save_values();
        source.close();
	}
	
	private void saveVariables(NetcdfFile source)
	{
		List<Future<?>> tasks = new ArrayList<>();

		for (Variable var : source.getVariables())
		{
			if (!this.variableIsApproved(var.getShortName()))
			{
				continue;
			}

			if (var.getDimensions().size() > 1 || var.getDimension(0).getLength() > 1)
			{
				VariableInserter inserter = new VariableInserter(var);
				inserter.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
				inserter.setOnRun(ProgressMonitor.onThreadStartHandler());
				tasks.add(Executor.execute(inserter));

				// TODO: This causes the code to only ingest a single variable. Fine for testing, but it needs to get removed
				if (!this.detailsAreSpecified()) {
					break;
				}
			}
		}

		for (Future<?> task : tasks)
        {
            try {
                task.get();
            }
            catch (InterruptedException e) {
                LOGGER.error("Could not complete a task because it was interuptted.");
                LOGGER.error(Strings.getStackTrace(e));
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
	}
	
	private static int getXLength(Variable var)
	{
		int length = 0;
		
		switch (var.getDimensions().size())
		{
			case 1:
				length = var.getDimension(0).getLength();
				break;
			case 2:
				length = var.getDimension(0).getLength();
				break;
			case 3:
				length = var.getDimension(1).getLength();
				break;				
		}
		
		return length;
	}
	
	private static Integer getYLength(Variable var)
	{
		Integer length = null;
		
		switch (var.getDimensions().size())
		{
			case 2:
				length = var.getDimension(1).getLength();
				break;
			case 3:
				length = var.getDimension(2).getLength();
				break;
		}
		
		return length;
	}
	
	private NetcdfFile getSource() throws IOException
	{
		if (source == null)
		{
			source = NetcdfFile.open(getAbsoluteFilename());
		}
		return source;
	}
	
	private NetcdfFile source = null;
	
	private boolean isObservation()
	{
		return this.lead == 0;
	}

	private Integer getLead()
	{
		if (this.lead == null)
		{
		    String description;

		    if (this.getRange().equalsIgnoreCase("analysis"))
            {
                description = Collections.find(getFileParts(), (String possibility) -> {
                    return possibility.startsWith("t") || possibility.endsWith("z");
                });
            }
            else
            {
                description = Collections.find(getFileParts(), (String possibility) -> {
                    return possibility.startsWith("f");
                });
            }

			
			description = description.replaceAll("\\D", "");
			this.lead = Integer.parseInt(description);
		}
		return this.lead;
	}
	
	private String getRange()
	{
		if (this.range == null)
		{
			this.range = Collections.find(getFileParts(), (String possibility) -> {
				return possibility.endsWith("range") || possibility.endsWith("assim");
			}).split("_")[0];
		}
		return this.range;
	}
	
	protected String getDataCategory()
	{
		if (this.data_category == null)
		{
			String category = Collections.find(getFileParts(), (String possibility) -> {
				return possibility.contains("land") || possibility.contains("reservoir");
			});
			
			this.data_category = category.split("_")[0];
		}
		return this.data_category;
	}
	
	private String[] getFileParts()
	{
		if (fileParts == null)
		{
			Path sourcePath = Paths.get(getAbsoluteFilename());
			String filename = sourcePath.getFileName().toString();
			filename = filename.replaceAll("\\.gz", "");
			filename = filename.replaceAll("nwm\\.", "");
			filename = filename.replaceAll("\\.nc", "");
			
			fileParts = filename.split("\\.");
		}
		
		return this.fileParts;
	}
	
	private Integer getForecastID() throws Exception
	{
		if (this.forecastId == null)
		{
		    ForecastDetails details = new ForecastDetails(this.getFilename());

		    details.setForecastDate(this.modelInitializationTime);

		    details.setCreationDate(this.modelInitializationTime);
		    details.setLead(this.getLead());
		    details.setRange(this.getRange());

		    this.forecastId = details.getForecastID();
		}
		return this.forecastId;
	}

	private int getSourceID() throws Exception
    {
        if (this.sourceID == 0)
        {
            this.sourceID = DataSources.getSourceID(this.getAbsoluteFilename(), this.modelInitializationTime, this.getLead());
        }
        return this.sourceID;
    }

    private int getEnsembleID()
    {
        if (this.ensembleID == 0)
        {
            try {
                this.ensembleID = Ensembles.getEnsembleID(this.ensembleName);
            } catch (Exception e) {
                System.err.println("The ID for the ensemble named: '" + this.ensembleName + "' could not be retrieved from the database.");
                e.printStackTrace();
            }
        }
        return this.ensembleID;
    }

    private Double getMissingValue()
    {
        return this.missingValue;
    }

	private String[] fileParts = null;
	private String range = null;
	private String data_category = null;
	private Integer lead = null;
	private int sourceID;

	private String ensembleName = "default";
	private int ensembleID;
	
	private String modelInitializationTime;
	private Double missingValue = null;
	private Integer forecastId = null;
}
