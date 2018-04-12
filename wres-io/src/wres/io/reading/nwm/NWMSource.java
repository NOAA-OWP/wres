package wres.io.reading.nwm;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * @author ctubbs
 *
 */
public class NWMSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NWMSource.class);

	/**
     *
     * @param projectConfig the ProjectConfig causing ingest
	 * @param filename the file name
	 */
	public NWMSource( ProjectConfig projectConfig, String filename )
    {
        super( projectConfig );
		this.setFilename(filename);
		this.setHash();
	}

	@Override
	public List<IngestResult> save() throws IOException
	{
		try ( NetcdfFile source = NetcdfFile.open(getAbsoluteFilename()) )
		{
			saveNetCDF( source );
		}

		return IngestResult.singleItemListFrom( this.getProjectConfig(),
												this.getDataSourceConfig(),
												this.getHash(),
												false );
	}

	@Override
	protected Logger getLogger()
	{
		return NWMSource.LOGGER;
	}

    private void saveNetCDF( NetcdfFile source ) throws IOException
	{
		Variable var = NetCDF.getVariable(source, this.getSpecifiedVariableName());

		if (var != null)
        {
			WRESRunnable saver;
			if (NetCDF.isGridded(var))
			{
				saver = new GriddedNWMValueSaver( this.getFilename(),
												  this.getFutureHash(),
												  ConfigHelper.isForecast(this.dataSourceConfig));
			}
			else
			{
				saver = new VectorNWMValueSaver( this.getFilename(),
												 this.getFutureHash(),
												 this.dataSourceConfig );
			}

			saver.setOnRun(ProgressMonitor.onThreadStartHandler());
			saver.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
			Database.ingest(saver);
        }
        else
        {
            LOGGER.debug( "The NetCDF file at '{}' did not contain the "
                          + "requested variable.", this.getFilename() );
        }
	}
}
