package wres.io.reading.nwm;

import java.io.IOException;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.io.concurrency.WRESRunnable;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
@Internal(exclusivePackage = "wres.io")
public class NWMSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NWMSource.class);

	/**
	 * 
	 * @param filename the file name
	 */
	@Internal(exclusivePackage = "wres.io")
	public NWMSource( String filename)
    {
		this.setFilename(filename);
		this.setHash();
	}
	
	@Override
	public void saveObservation() throws IOException
	{
		try ( NetcdfFile source = getSource() )
		{
			save( source );
		}
	}
	
	@Override
	public void saveForecast() throws IOException
	{
		try (NetcdfFile source = getSource())
		{
			save( source );
		}
	}
	
	private void save(NetcdfFile source)
	{
		Variable var = NetCDF.getVariable(source, this.getSpecifiedVariableName());

		if (var != null)
        {
            try
			{
                WRESRunnable saver;
                if (NetCDF.isGridded(var))
                {
                    saver = new GriddedNWMValueSaver( this.getFilename(),
													  Variables.getVariableID(var.getShortName(),
																				var.getUnitsString()),
													  this.getFutureHash());
                }
                else
                {
                    saver = new VectorNWMValueSaver( this.getFilename(),
													 this.getFutureHash(),
													 this.dataSourceConfig,
													 this.getProjectDetails());
                }

                saver.setOnRun(ProgressMonitor.onThreadStartHandler());
                saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                Database.ingest(saver);
            }
            catch (SQLException e)
			{
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
}
