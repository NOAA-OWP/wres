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
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.TimeScales;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.reading.PreIngestException;
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

    private final Features featuresCache;
    private final TimeScales timeScalesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final DataSources dataSourcesCache;
    private boolean alreadyFound;

	/**
     *
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param dataSourcesCache The data sources cache to use.
     * @param projectConfig The ProjectConfig causing ingest
	 * @param dataSource The data source information
	 * @param featuresCache The features cache
	 * @param timeScalesCache The time scales cache
	 * @param measurementUnitsCache The measurement unis cache
	 */
	public NWMSource( SystemSettings systemSettings,
                      Database database,
                      DataSources dataSourcesCache,
                      Features featuresCache,
                      TimeScales timeScalesCache,
                      MeasurementUnits measurementUnitsCache,
                      ProjectConfig projectConfig,
                      DataSource dataSource )
    {
        super( projectConfig, dataSource );
        this.systemSettings = systemSettings;
        this.database = database;
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.timeScalesCache = timeScalesCache;
        this.measurementUnitsCache = measurementUnitsCache;
	}

	private SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

	private Database getDatabase()
    {
        return this.database;
    }

    private DataSources getDataSourcesCache()
    {
        return this.dataSourcesCache;
    }

    private Features getFeaturesCache()
    {
        return this.featuresCache;
    }

    private TimeScales getTimeScalesCache()
    {
        return this.timeScalesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
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
                hash = NetCDF.getGriddedUniqueIdentifier( source,
                                                          this.getFilename(),
                                                          var.getShortName() );

                try
                {
                    DataSources dataSources = this.getDataSourcesCache();
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
                                                  this.getFeaturesCache(),
                                                  this.getMeasurementUnitsCache(),
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
