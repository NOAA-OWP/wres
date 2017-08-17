package wres.io.reading.ucar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.io.concurrency.Executor;
import wres.io.concurrency.WRESRunnable;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.utilities.Database;
import wres.util.*;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author ctubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public class NetCDFSource extends BasicSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetCDFSource.class);

	/**
	 * 
	 */
	@Internal(exclusivePackage = "wres.io")
	public NetCDFSource(String filename) {
		this.setFilename(filename);
	}
	
	@Override
	public void saveObservation() throws IOException
	{
		throw new NotImplementedException("Observational data within NetCDF files are not supported by the WRES.");
	}
	
	@Override
	public void saveForecast() throws IOException
	{
		NetcdfFile source = getSource();

		Attribute attr = source.findGlobalAttributeIgnoreCase("missing_value");
		if (attr != null)
		{
			this.missingValue = attr.getNumericValue().doubleValue();
		}

		save(source);
        source.close();
	}
	
	private void save(NetcdfFile source)
	{
		Variable var = NetCDF.getVariable(source, this.getSpecifiedVariableName());

		if (var != null)
        {
            try {
                WRESRunnable saver;
                if (NetCDF.isGridded(var)) {
                    saver = new GriddedNetCDFValueSaver(this.getFilename(),
                                                        Variables.getVariableID(var.getShortName(), var.getUnitsString()),
                                                        this.getMissingValue());
                }
                else
                {
                    saver = new VectorNetCDFValueSaver(this.getFilename(), var.getShortName());
                }

                saver.setOnRun(ProgressMonitor.onThreadStartHandler());
                saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                Database.storeIngestTask(Executor.execute(saver));
            }
            catch (SQLException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }
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

    private Double getMissingValue()
    {
        return this.missingValue;
    }
	private Double missingValue = null;
}
