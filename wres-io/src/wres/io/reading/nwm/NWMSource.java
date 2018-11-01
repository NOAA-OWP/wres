package wres.io.reading.nwm;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.WRESRunnable;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceDetails;
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
    private static final int MAXIMUM_OPEN_ATTEMPTS = 5;

    private boolean alreadyFound;

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
	    String hash;
	    int tryCount = 0;


	    while (true)
        {
            try ( NetcdfFile source = NetcdfFile.open( this.getFilename() ) )
            {
                hash = saveNetCDF( source );
                break;
            }
            catch (IOException exception)
            {
                if (exception.getCause() instanceof SocketTimeoutException &&
                    tryCount < MAXIMUM_OPEN_ATTEMPTS)
                {
                    LOGGER.error("Connection to the NWM file at '{}' failed.", this.getFilename());
                    tryCount++;
                    continue;
                }

                throw exception;
            }
        }


		return IngestResult.singleItemListFrom( this.getProjectConfig(),
												this.getDataSourceConfig(),
												hash,
												this.getAbsoluteFilename(),
												this.alreadyFound );
	}

	@Override
	protected Logger getLogger()
	{
		return NWMSource.LOGGER;
	}

    private String saveNetCDF( NetcdfFile source ) throws IOException
	{
		Variable var = NetCDF.getVariable(source, this.getSpecifiedVariableName());
		String hash = this.getHash();

		if (var != null)
        {
			WRESRunnable saver;
			if(NetCDF.isGridded( var ))
            {
                Integer gridProjectionId;
                try
                {
                    gridProjectionId = GridManager.addGrid( source );
                }
                catch ( SQLException e )
                {
                    throw new IOException(
                            "Metadata about the grid in '" +
                            source.getLocation() +
                            "' could not be saved.", e
                    );
                }

                hash = NetCDF.getGriddedUniqueIdentifier( source, this.filename );

                try
                {
                    SourceDetails sourceDetails = DataSources.getExistingSource( hash );

                    if(sourceDetails != null && Files.exists( Paths.get( sourceDetails.getSourcePath()) ))
                    {
                        this.setFilename( sourceDetails.getSourcePath() );
                        this.alreadyFound = true;
                        return hash;
                    }
                }
                catch ( SQLException e )
                {
                    throw new IOException( "Could not check to see if gridded data is already present.", e );
                }

                saver = new GriddedNWMValueSaver( this.getFilename(), hash, gridProjectionId);
            }
			else
            {
                saver = new VectorNWMValueSaver( this.getFilename(),
                                                 this.getHash(),
                                                 this.dataSourceConfig );
            }

			saver.setOnRun(ProgressMonitor.onThreadStartHandler());
			saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler());

            saver.run();
			//Database.ingest(saver);
        }
        else
        {
            throw new IOException( "The NetCDF file at '" +
                                   this.getFilename() +
                                   "' did not contain the " +
                                   "requested variable.");
        }

        return hash;
	}
}
