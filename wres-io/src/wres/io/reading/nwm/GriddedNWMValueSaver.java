package wres.io.reading.nwm;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;

import wres.io.concurrency.WRESRunnable;
import wres.io.data.details.SourceDetails;
import wres.util.NetCDF;
import wres.util.NotImplementedException;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
class GriddedNWMValueSaver extends WRESRunnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GriddedNWMValueSaver.class);

    private final String fileName;
    private NetcdfFile source;
    private String hash;
    private final Future<String> futureHash;

    GriddedNWMValueSaver( String fileName, Future<String> futureHash)
	{
		this.fileName = fileName;
		this.futureHash = futureHash;
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

            Instant outputTime = NetCDF.getReferenceTime( this.getFile() );
			Integer lead = NetCDF.getLeadTime( this.getFile() );

            SourceDetails griddedSource = new SourceDetails(  );
			griddedSource.setSourcePath( this.fileName );

			griddedSource.setOutputTime( outputTime.toString() );
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
