package wres.io.reading.nwm;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import wres.config.generated.ProjectConfig;
import wres.io.concurrency.WRESCallable;
import wres.io.data.caching.Caches;
import wres.io.data.caching.DataSources;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
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

    private final SystemSettings systemSettings;
    private final Database database;

    private final Caches caches;
    private boolean alreadyFound;

	/**
     *
     * @param systemSettings The system settings
     * @param database The database
     * @param caches The database caches/ORMs
     * @param projectConfig The ProjectConfig causing ingest
	 * @param dataSource The data source information
	 */
	public NWMSource( SystemSettings systemSettings,
                      Database database,
                      Caches caches,
                      ProjectConfig projectConfig,
                      DataSource dataSource )
    {
        super( projectConfig, dataSource );
        this.systemSettings = systemSettings;
        this.database = database;
        this.caches = caches;
	}

	private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

	private Database getDatabase()
    {
        return this.database;
    }

    private Caches getCaches()
    {
        return this.caches;
    }

    @Override
	public List<IngestResult> save() throws IOException
	{
	    int tryCount = 0;

        List<IngestResult> saved;

	    while (true)
        {
            try ( NetcdfFile source = NetcdfFiles.open( this.getFilename().toString() ) )
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
	
    private List<IngestResult> saveNetCDF( NetcdfFile source ) throws IOException
	{
		Variable var = NetCDF.getVariable(source, this.getSpecifiedVariableName());
		String hash = this.getHash();

		if (var != null)
        {		    
			WRESCallable<List<IngestResult>> saver;

			if(NetCDF.isGridded( var ))
            {
                hash = NetCDF.getGriddedUniqueIdentifier( source,
                                                          this.getFilename(),
                                                          var.getShortName() );

                try
                {
                    DataSources dataSources = this.getCaches()
                                                  .getDataSourcesCache();
                    SourceDetails sourceDetails = dataSources.getExistingSource( hash );

                    if(sourceDetails != null && Files.exists( Paths.get( sourceDetails.getSourcePath()) ))
                    {
                        // Was setting the file name important? Seems as though
                        // the filename should be immutable.
                        //this.setFilename( sourceDetails.getSourcePath() );
                        this.alreadyFound = true;
                        SourceCompletedDetails completedDetails =
                                new SourceCompletedDetails( this.getDatabase(), sourceDetails );
                        boolean completed = completedDetails.wasCompleted();
                        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                                this.getDataSource(),
                                                                sourceDetails.getId(),
                                                                !this.alreadyFound,
                                                                !completed );
                    }
                }
                catch ( SQLException e )
                {
                    throw new IOException( "Could not check to see if gridded data is already present.", e );
                }

                saver = new GriddedNWMValueSaver( this.getSystemSettings(),
                                                  this.getDatabase(),
                                                  this.getCaches()
                                                      .getFeaturesCache(),
                                                  this.getCaches()
                                                      .getMeasurementUnitsCache(),
                                                  this.getProjectConfig(),
                                                  this.getDataSource(),
                                                  hash );
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
            List<String> variableNames = source.getVariables()
                                               .stream()
                                               .map( Variable::getShortName )
                                               .collect( Collectors.toList());
            throw new PreIngestException( "The NetCDF file at '" +
                                          this.getFilename()
                                          + "' did not contain the "
                                          + "requested variable, "
                                          + this.getSpecifiedVariableName()
                                          + ". Available variables: "
                                          + variableNames );
        }
	}
}
