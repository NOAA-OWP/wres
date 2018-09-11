package wres.io.reading.nwm;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.FileWriter2;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.system.ProgressMonitor;
import wres.util.NetCDF;

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
	}

	@Override
	public List<IngestResult> save() throws IOException
	{
        // TODO: Determine handling for the case where the "file" name is actually a URL
		try ( NetcdfFile source = NetcdfFile.open(this.getFilename()) )
		{
			saveNetCDF( source );
		}

		return IngestResult.singleItemListFrom( this.getProjectConfig(),
												this.getDataSourceConfig(),
												this.getHash(),
												this.getAbsoluteFilename(),
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
			if (NetCDF.isGridded(var) && this.getHash() == null)
			{
			    /*We can't use the file name as the key because that is the URL. If we can strip the domain from the url, maybe we can use that
                   to form the actual file path. I'll need to consult the other code and make sure everything stays in line. */
			    // TODO: Return to this once the object store can be used again
			    /*if ( this.getIsRemote())
                {
                    URL sourcePath = new URL(source.getLocation());
                    ReadableByteChannel channel = Channels.newChannel(sourcePath.openStream());
                }*/
			    // TODO: We aren't guaranteed to have a physical file.
                // If we don't we need to save it for later use
				saver = new GriddedNWMValueSaver( this.getFilename(), this.getHash());
			}
			else if(NetCDF.isGridded( var ))
            {
                saver = new GriddedNWMValueSaver( this.getFilename(),
                                                  this.getHash());
            }
			else if (this.getHash() == null)
			{
				saver = new VectorNWMValueSaver( this.getFilename(),
												 this.getFutureHash(),
												 this.dataSourceConfig );
			}
			else
            {
                saver = new VectorNWMValueSaver( this.getFilename(),
                                                 this.getHash(),
                                                 this.dataSourceConfig );
            }

			saver.setOnRun(ProgressMonitor.onThreadStartHandler());
			saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler());
			Database.ingest(saver);
        }
        else
        {
            LOGGER.debug( "The NetCDF file at '{}' did not contain the "
                          + "requested variable.", this.getFilename() );
        }
	}
}
