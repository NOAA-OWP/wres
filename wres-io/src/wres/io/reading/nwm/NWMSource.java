package wres.io.reading.nwm;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.InvalidRangeException;
import ucar.nc2.FileWriter2;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.grid.GridDataset;
import ucar.unidata.geoloc.LatLonPoint;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.DataBuilder;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.NetCDF;

/**
 * @author ctubbs
 *
 */
public class NWMSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NWMSource.class);
    private static final int MAXIMUM_OPEN_ATTEMPTS = 5;

    private static final Object PROJECTION_LOCK = new Object();
    private static final Set<Integer> ENCOUNTERED_PROJECTIONS = new TreeSet<>();

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
                    LOGGER.error("Connection to NWM file failed.");
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
			Database.ingest(saver);
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
