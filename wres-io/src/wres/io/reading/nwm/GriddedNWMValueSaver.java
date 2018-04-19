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
import ucar.ma2.InvalidRangeException;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.DataSourceConfig;
import wres.io.concurrency.CopyExecutor;
import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnable;
import wres.io.concurrency.WRESRunnableException;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceDetails;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.NetCDF;
import wres.util.NotImplementedException;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.TimeHelper;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
class GriddedNWMValueSaver extends WRESRunnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GriddedNWMValueSaver.class);

    private int sourceID = Integer.MIN_VALUE;
    private final String fileName;
    private NetcdfFile source;
    private String hash;
    private final Future<String> futureHash;
    private final boolean isForecast;

    GriddedNWMValueSaver( String fileName, Future<String> futureHash, boolean isForecast)
	{
		this.fileName = fileName;
		this.futureHash = futureHash;
		this.isForecast = isForecast;
	}

	@Override
    public void execute() throws IOException, SQLException
    {
		try
        {
			String hash = null;

            try
            {
                hash = this.getHash();
            }
            catch ( ExecutionException e )
            {
                throw new IOException( e );
            }
            catch ( InterruptedException e )
            {
                this.getLogger().error("Gridded Data Ingest was interrupted for: {}", this.fileName);
                Thread.currentThread().interrupt();
            }

            String referenceTime = NetCDF.getInitializedTime( this.getFile() );
			int lead = 0;

			if ( this.isForecast )
            {
                lead = NetCDF.getLeadTime( this.getFile() );
            }

            SourceDetails griddedSource = new SourceDetails(  );
			griddedSource.setSourcePath( this.fileName );
			griddedSource.setOutputTime( referenceTime );
			griddedSource.setLead( lead );
			griddedSource.setHash( hash );
			griddedSource.setIsPointData( false );

			griddedSource.save();

			if ( griddedSource.getId() == null)
            {
                throw new IOException( "Information about the gridded data source at " +
                                       this.fileName + " could not be ingested." );
            }
		}
        finally
		{
            try
            {
                this.closeFile();
            }
            catch ( IOException e )
            {
                // Exception on close should not affect primary outputs.
                LOGGER.warn( "Failed to close file {}.", this.fileName, e );
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

    @Override
    protected Logger getLogger () {
        return GriddedNWMValueSaver.LOGGER;
    }
}
