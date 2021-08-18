package wres.io.reading.nwm;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import wres.config.generated.ProjectConfig;
import wres.io.concurrency.Downloader;
import wres.io.concurrency.WRESCallable;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.utilities.Database;
import wres.system.SystemSettings;
import wres.util.NetCDF;
import wres.util.TimeHelper;

/**
 * Executes the database copy operation for every value in the passed in string
 * @author Christopher Tubbs
 */
public class GriddedNWMValueSaver extends WRESCallable<List<IngestResult>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GriddedNWMValueSaver.class);

    private SystemSettings systemSettings;
    private Database database;
    private ProjectConfig projectConfig;
    private DataSource dataSource;
    private URI fileName;
    private NetcdfFile source;
    private final String hash;

	GriddedNWMValueSaver( SystemSettings systemSettings,
                          Database database,
                          ProjectConfig projectConfig,
                          DataSource dataSource,
                          final String hash )
    {
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( database );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( hash );
        this.systemSettings = systemSettings;
        this.database = database;
        this.projectConfig = projectConfig;
        this.dataSource = dataSource;
        this.fileName = dataSource.getUri();
        this.hash = hash;
    }

    private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    private Database getDatabase()
    {
        return this.database;
    }

	@Override
    public List<IngestResult> execute() throws IOException, SQLException
    {
        this.ensureFileIsLocal();

		try
        {
            Instant outputTime = NetCDF.getReferenceTime( this.getFile() );
			Duration lead = NetCDF.getLeadTime( this.getFile() );

            SourceDetails griddedSource = new SourceDetails(  );
			griddedSource.setSourcePath( this.fileName );

			griddedSource.setOutputTime( outputTime.toString() );
			griddedSource.setLead( TimeHelper.durationToLead(lead) );
			griddedSource.setHash( this.hash );
			griddedSource.setIsPointData( false );

			Database database = this.getDatabase();
			griddedSource.save( database );

			if ( griddedSource.getId() == null)
            {
                throw new IOException( "Information about the gridded data source at " +
                                       this.fileName + " could not be ingested." );
            }

            SourceCompletedDetails completedDetails =
                    new SourceCompletedDetails( this.getDatabase(),
                                                griddedSource );
			boolean complete;

			if ( griddedSource.performedInsert() )
            {
                completedDetails.markCompleted();
                complete = true;
            }
			else
            {
                complete = completedDetails.wasCompleted();
            }

			return IngestResult.singleItemListFrom( this.projectConfig,
                                                    this.dataSource,
                                                    griddedSource.getId(),
                                                    !griddedSource.performedInsert(),
                                                    !complete );
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

        if ( this.fileName.getScheme() != null &&
             this.fileName.getScheme().startsWith( "http" ) )
        {
            URL url = this.fileName.toURL();
            HttpURLConnection huc = (HttpURLConnection)url.openConnection();
            huc.setRequestMethod( "HEAD" );
            huc.setInstanceFollowRedirects( false );

            if (huc.getResponseCode() == HttpURLConnection.HTTP_OK)
            {
                this.retrieveFile( path );
            }
        }
        else
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

        final URI originalPath = this.fileName;

        SystemSettings systemSettings = this.getSystemSettings();
        this.fileName = Paths.get(
                systemSettings.getNetCDFStorePath(),
                path.subpath( firstNameIndex, nameCount ).toString()
        ).toUri();

        if ( !Paths.get( this.fileName.toString(), path.getFileName().toString() ).toFile().exists())
        {
            Downloader downloader = new Downloader( Paths.get( this.fileName ), originalPath );
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
            this.source = NetcdfFiles.open( this.fileName.toString() );
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
