package wres.io.reading.nwm;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;

import wres.io.concurrency.Downloader;
import wres.io.concurrency.WRESRunnable;
import wres.io.data.details.SourceDetails;
import wres.system.SystemSettings;
import wres.util.NetCDF;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
public class GriddedNWMValueSaver extends WRESRunnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GriddedNWMValueSaver.class);

    private String fileName;
    private NetcdfFile source;
    private final String hash;
    private final int gridProjectionId;

	GriddedNWMValueSaver(String fileName, final String hash, final int gridProjectionId)
    {
        this.fileName = fileName;
        this.hash = hash;
        this.gridProjectionId = gridProjectionId;
    }

	@Override
    public void execute() throws IOException, SQLException
    {
        this.ensureFileIsLocal();

		try
        {
            Instant outputTime = NetCDF.getReferenceTime( this.getFile() );
			Integer lead = NetCDF.getLeadTime( this.getFile() );

            SourceDetails griddedSource = new SourceDetails(  );
			griddedSource.setSourcePath( this.fileName );

			griddedSource.setOutputTime( outputTime.toString() );
			griddedSource.setLead( lead );
			griddedSource.setHash( this.hash );
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

	private void ensureFileIsLocal() throws IOException
    {
        Path path = Paths.get( this.fileName);

        try
        {
            URL url = new URL(this.fileName);
            HttpURLConnection huc = (HttpURLConnection)url.openConnection();
            huc.setRequestMethod( "HEAD" );
            huc.setInstanceFollowRedirects( false );

            if (huc.getResponseCode() == HttpURLConnection.HTTP_OK)
            {
                this.retrieveFile( path );
            }
        }
        catch ( MalformedURLException e )
        {
            LOGGER.trace("It was determined that {} is not remote data.", this.fileName);

            if(!Files.exists( path ))
            {
                throw new IOException( "Gridded data could not be found at: '" + this.fileName + "'" );
            }
        }
    }

    private void retrieveFile(final Path path) throws IOException
    {
        Integer nameCount = path.getNameCount();
        Integer firstNameIndex = 0;

        if (nameCount > 4)
        {
            firstNameIndex = nameCount - 4;
        }

        final String originalPath = this.fileName;

        this.fileName = Paths.get(
                SystemSettings.getNetCDFStorePath(),
                path.subpath( firstNameIndex, nameCount ).toString()
        ).toString();

        if ( !Paths.get( this.fileName, path.getFileName().toString() ).toFile().exists())
        {
            Downloader downloader = new Downloader(Paths.get(this.fileName), originalPath);
            downloader.setDisplayOutput( false );
            downloader.execute();

            if (!downloader.fileHasBeenDownloaded())
            {
                throw new IOException( "The file at '" + originalPath + "' could not be downloaded." );
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

    @Override
    protected Logger getLogger () {
        return GriddedNWMValueSaver.LOGGER;
    }
}
