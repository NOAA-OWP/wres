package wres.io.reading.nwm;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import ucar.nc2.Variable;

import wres.config.generated.ProjectConfig;
import wres.config.generated.UnnamedFeature;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Caches;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.GriddedFeatures;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.IngestResultInMemory;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.retrieval.DataAccessException;
import wres.io.utilities.Database;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.NetCDF;

/**
 * Reads a gridded NWM source. Note that, when ingesting time-series using the {@link TimeSeriesIngester}, this class
 * does not perform any composition of time-series. Indeed, it can only see a single source/grid. Thus, the correct 
 * composition must happen in the {@link TimeSeriesIngester} implementation or subsequently.
 * 
 * @author ctubbs
 * @author James Brown
 */
public class GriddedNWMSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( GriddedNWMSource.class );
    private static final int MAXIMUM_OPEN_ATTEMPTS = 5;

    private final SystemSettings systemSettings;
    private final Database database;

    private final Caches caches;
    private boolean alreadyFound;
    private final TimeSeriesIngester timeSeriesIngester;
    private static final Object FEATURE_GUARD = new Object();
    private static final Set<FeatureKey> GRIDDED_FEATURES = new HashSet<>();
    private static final AtomicReference<ProjectConfig> FEATURE_REFERENCE = new AtomicReference<>();

    /**
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings The system settings
     * @param database The database
     * @param caches The database caches/ORMs
     * @param projectConfig The ProjectConfig causing ingest
     * @param dataSource The data source information
     */
    public GriddedNWMSource( TimeSeriesIngester timeSeriesIngester,
                             SystemSettings systemSettings,
                             Database database,
                             Caches caches,
                             ProjectConfig projectConfig,
                             DataSource dataSource )
    {
        super( projectConfig, dataSource );

        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );

        if ( !systemSettings.isInMemory() )
        {
            Objects.requireNonNull( database );
            Objects.requireNonNull( caches );
        }

        if ( Objects.isNull( projectConfig.getPair().getGridSelection() ) )
        {
            throw new IllegalArgumentException( "Cannot create an instance without a grid data selection." );
        }

        this.systemSettings = systemSettings;
        this.database = database;
        this.caches = caches;
        this.timeSeriesIngester = timeSeriesIngester;
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

        while ( true )
        {
            try ( NetcdfFile source = NetcdfFiles.open( this.getFilename().toString() ) )
            {
                saved = saveNetCDF( source );
                break;
            }
            catch ( IOException exception )
            {
                if ( exception.getCause() instanceof SocketTimeoutException &&
                     tryCount < MAXIMUM_OPEN_ATTEMPTS )
                {
                    LOGGER.error( "Connection to the NWM file at '{}' failed.", this.getFilename() );
                    tryCount++;
                    continue;
                }

                throw exception;
            }
        }

        return saved;
    }

    /**
     * @return the time-series ingester
     */

    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }

    /**
     * @param source the source
     * @return the ingest results
     * @throws NullPointerException if the source is null
     * @throws IOException if the data could not be ingested for any other reason
     */

    private List<IngestResult> saveNetCDF( NetcdfFile source ) throws IOException
    {
        Objects.requireNonNull( source );

        Variable var = NetCDF.getVariable( source, this.getSpecifiedVariableName() );
        String hash = this.getHash();

        if ( var != null )
        {
            if ( !NetCDF.isGridded( var ) )
            {
                throw new UnsupportedOperationException( "Vector netCDF ingest now uses a different declaration. "
                                                         + "Please use source declaration like '<source interface=\""
                                                         + "nwm_short_range_channel_rt_conus\">data/nwmVector/"
                                                         + "</source>' instead" );
            }

            WRESCallable<List<IngestResult>> saver;

            hash = NetCDF.getGriddedUniqueIdentifier( source,
                                                      this.getFilename(),
                                                      var.getShortName() );

            // In memory evaluation?
            // TODO: if/when gridded time-series are treated like any other time-series, this special handling of
            // gridded ingest can be removed along with the GriddedNWMValueSaver below. See #51232.
            if ( this.systemSettings.isInMemory() )
            {
                this.ingestTimeSeries( source, this.getTimeSeriesIngester(), this.getProjectConfig() );

                LOGGER.debug( "Return an in-memory ingest result, no gridded metadata ingest required." );
                return List.of( new IngestResultInMemory( this.getDataSource() ) );
            }

            try
            {
                DataSources dataSources = this.getCaches()
                                              .getDataSourcesCache();
                SourceDetails sourceDetails = dataSources.getExistingSource( hash );

                if ( sourceDetails != null && Files.exists( Paths.get( sourceDetails.getSourcePath() ) ) )
                {
                    // Was setting the file name important? Seems as though
                    // the filename should be immutable.
                    //this.setFilename( sourceDetails.getSourcePath() );
                    this.alreadyFound = true;
                    SourceCompletedDetails completedDetails =
                            new SourceCompletedDetails( this.getDatabase(), sourceDetails );
                    boolean completed = completedDetails.wasCompleted();
                    return IngestResult.singleItemListFrom( this.getDataSource(),
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
                                              this.getDataSource(),
                                              hash );

            saver.setOnRun( ProgressMonitor.onThreadStartHandler() );
            saver.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

            return saver.call();
        }
        else
        {
            List<String> variableNames = source.getVariables()
                                               .stream()
                                               .map( Variable::getShortName )
                                               .collect( Collectors.toList() );
            throw new PreIngestException( "The NetCDF file at '" +
                                          this.getFilename()
                                          + "' did not contain the "
                                          + "requested variable, "
                                          + this.getSpecifiedVariableName()
                                          + ". Available variables: "
                                          + variableNames );
        }
    }

    /**
     * Ingest the gridded data.
     * @param file the file source
     * @param ingester the ingester
     * @param projectConfig the project declaration
     * @throws IOException 
     */

    private void ingestTimeSeries( NetcdfFile file, TimeSeriesIngester ingester, ProjectConfig projectConfig )
            throws IOException
    {
        // The gridded features are read only once per evaluation under the assumption that every source has the 
        // same grid specification. When a new project arrives, the cache is cleared. This is required for performance. 
        // The following shenanigans implement that goal.
        synchronized ( FEATURE_GUARD )
        {
            // Clear existing features when a new project arrives
            ProjectConfig oldValue = FEATURE_REFERENCE.getAndSet( projectConfig );
            if ( Objects.nonNull( oldValue ) && oldValue != projectConfig )
            {
                LOGGER.debug( "Clearing gridded features cache because a new project arrived." );

                FEATURE_REFERENCE.set( projectConfig );
                GRIDDED_FEATURES.clear();
            }

            // Gridded features needed?
            if ( GRIDDED_FEATURES.isEmpty() )
            {
                List<UnnamedFeature> gridSelection = projectConfig.getPair()
                                                                  .getGridSelection();

                try
                {
                    GriddedFeatures features = new GriddedFeatures.Builder( gridSelection ).addFeatures( file )
                                                                                           .build();

                    GRIDDED_FEATURES.addAll( features.get() );

                    LOGGER.debug( "Identified these gridded features whose time-series data should be ingested: {}.",
                                  features.get() );
                }
                catch ( IOException e )
                {
                    throw new DataAccessException( "Unable to determine the gridded features to ingest." );
                }
            }
        }

        // Prepare the grid data request
        Path path = Paths.get( this.getDataSource().getUri() );
        String pathString = path.toString();
        TimeScaleOuter timeScale = null;

        if ( Objects.nonNull( this.getDataSourceConfig().getExistingTimeScale() ) )
        {
            timeScale = TimeScaleOuter.of( this.getDataSourceConfig().getExistingTimeScale() );
        }

        // Time window constrained only by the pair declaration
        TimeWindowOuter timeWindow = TimeWindowGenerator.getOneBigTimeWindow( this.getProjectConfig()
                                                                                  .getPair() );

        Request request = Fetcher.prepareRequest( List.of( pathString ),
                                                  GRIDDED_FEATURES,
                                                  this.getDataSource()
                                                      .getVariable()
                                                      .getValue(),
                                                  timeWindow,
                                                  ConfigHelper.isForecast( this.getDataSourceConfig() ),
                                                  timeScale );

        // Acquire the response and ingest the time-series
        SingleValuedTimeSeriesResponse response = Fetcher.getSingleValuedTimeSeries( request );
        for ( Stream<TimeSeries<Double>> nextStream : response.getTimeSeries().values() )
        {
            nextStream.forEach( timeSeries -> this.getTimeSeriesIngester()
                                                  .ingestSingleValuedTimeSeries( timeSeries, this.getDataSource() ) );
        }
    }
}
