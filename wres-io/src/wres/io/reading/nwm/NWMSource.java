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
import wres.io.concurrency.WRESCallable;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.system.DatabaseLockManager;
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

    private final DatabaseLockManager lockManager;

    private boolean alreadyFound;

	/**
     *
     * @param projectConfig the ProjectConfig causing ingest
	 * @param dataSource the data source information
     * @param lockManager The lock manager to use.
	 */
	public NWMSource( ProjectConfig projectConfig,
                      DataSource dataSource,
                      DatabaseLockManager lockManager )
    {
        super( projectConfig, dataSource );
        this.lockManager = lockManager;
	}

	@Override
	public List<IngestResult> save() throws IOException
	{
	    int tryCount = 0;

        List<IngestResult> saved;

	    while (true)
        {
            try ( NetcdfFile source = NetcdfFile.open( this.getFilename().toString() ) )
            {
                saved = saveNetCDF( source );
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

		return saved;
	}

	@Override
	protected Logger getLogger()
	{
		return NWMSource.LOGGER;
	}

    private List<IngestResult> saveNetCDF( NetcdfFile source ) throws IOException
	{
		Variable var = NetCDF.getVariable(source, this.getSpecifiedVariableName());
		String hash = this.getHash();

		if (var != null)
        {
			WRESCallable<List<IngestResult>> saver;

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

                hash = NetCDF.getGriddedUniqueIdentifier( source, this.getFilename() );

                try
                {
                    SourceDetails sourceDetails = DataSources.getExistingSource( hash );

                    if(sourceDetails != null && Files.exists( Paths.get( sourceDetails.getSourcePath()) ))
                    {
                        // Was setting the file name important? Seems as though
                        // the filename should be immutable.
                        //this.setFilename( sourceDetails.getSourcePath() );
                        this.alreadyFound = true;
                        SourceCompletedDetails completedDetails =
                                new SourceCompletedDetails( sourceDetails );
                        boolean completed = completedDetails.wasCompleted();
                        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                                this.getDataSource(),
                                                                hash,
                                                                !this.alreadyFound,
                                                                !completed );
                    }
                }
                catch ( SQLException e )
                {
                    throw new IOException( "Could not check to see if gridded data is already present.", e );
                }

                saver = new GriddedNWMValueSaver( this.getProjectConfig(),
                                                  this.getDataSource(),
                                                  hash,
                                                  gridProjectionId );
            }
			else
            {
                throw new UnsupportedOperationException( "Vector netCDF ingest now uses a different declaration. Please use source declaration like '<source interface=\"nwm_short_range_channel_rt_conus\">data/nwmVector/</source>' instead" );
            }

			saver.setOnRun(ProgressMonitor.onThreadStartHandler());
			saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler());

            return saver.call();
        }
        else
        {
            throw new IOException( "The NetCDF file at '" +
                                   this.getFilename() +
                                   "' did not contain the " +
                                   "requested variable.");
        }
	}

	private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }
}
