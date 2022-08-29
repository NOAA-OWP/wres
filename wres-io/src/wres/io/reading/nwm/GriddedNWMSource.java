package wres.io.reading.nwm;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import wres.config.generated.DataSourceConfig;
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
import wres.io.config.ConfigHelper;
import wres.io.data.caching.GriddedFeatures;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.ingesting.memory.IngestResultInMemory;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.Source;
import wres.io.reading.TimeSeriesTuple;
import wres.io.retrieval.DataAccessException;
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
public class GriddedNWMSource implements Source
{
    private static final Logger LOGGER = LoggerFactory.getLogger( GriddedNWMSource.class );
    private static final int MAXIMUM_OPEN_ATTEMPTS = 5;
    private static final Object FEATURE_GUARD = new Object();
    private static final Set<FeatureKey> GRIDDED_FEATURES = new HashSet<>();
    private static final AtomicReference<ProjectConfig> FEATURE_REFERENCE = new AtomicReference<>();
    
    private final SystemSettings systemSettings;
    private final TimeSeriesIngester timeSeriesIngester;
    private final DataSource dataSource;
    private final ProjectConfig projectConfig;

    /**
     * @param timeSeriesIngester the time-series ingester
     * @param systemSettings The system settings
     * @param projectConfig The ProjectConfig causing ingest
     * @param dataSource The data source information
     */
    public GriddedNWMSource( TimeSeriesIngester timeSeriesIngester,
                             SystemSettings systemSettings,
                             ProjectConfig projectConfig,
                             DataSource dataSource )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );

        if ( Objects.isNull( projectConfig.getPair().getGridSelection() ) )
        {
            throw new IllegalArgumentException( "Cannot create an instance without a grid data selection." );
        }

        this.systemSettings = systemSettings;
        this.timeSeriesIngester = timeSeriesIngester;
        this.dataSource = dataSource;
        this.projectConfig = projectConfig;
    }

    /**
     * @return the data source
     */
    private DataSource getDataSource()
    {
        return this.dataSource;
    }
    
    /**
     * @return the project declaration
     */
    private ProjectConfig getProjectConfig()
    {
        return this.projectConfig;
    }
    
    @Override
    public List<IngestResult> save()
    {
        int tryCount = 0;

        List<IngestResult> saved;

        while ( true )
        {
            try ( NetcdfFile source = NetcdfFiles.open( this.getFileName().toString() ) )
            {
                saved = saveNetCDF( source );
                break;
            }
            catch ( IOException exception )
            {
                if ( exception.getCause() instanceof SocketTimeoutException &&
                     tryCount < MAXIMUM_OPEN_ATTEMPTS )
                {
                    LOGGER.error( "Connection to the NWM file at '{}' failed.", this.getFileName() );
                    tryCount++;
                }
                else
                {
                    throw new ReadException( "Failed to read a gridded NWM source.", exception );
                }
            }
        }

        return saved;
    }

    /**
     * @return the file name
     */
    
    private URI getFileName()
    {
        return this.getDataSource()
                   .getUri();
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

        String variableName = ConfigHelper.getVariableName( this.getDataSource()
                                                                .getContext() );
        
        if ( Objects.nonNull( variableName ) )
        {
            Variable var = NetCDF.getVariable( source, variableName );

            if ( !NetCDF.isGridded( var ) )
            {
                throw new UnsupportedOperationException( "Vector netCDF ingest now uses a different declaration. "
                                                         + "Please use source declaration like '<source interface=\""
                                                         + "nwm_short_range_channel_rt_conus\">data/nwmVector/"
                                                         + "</source>' instead" );
            }

            // In memory evaluation?
            // TODO: if/when gridded time-series are treated like any other time-series, this special handling of
            // gridded ingest can be removed along with the GriddedNWMValueSaver below. See #51232.
            if ( this.systemSettings.isInMemory() )
            {
                this.ingestTimeSeries( source, this.getTimeSeriesIngester(), this.getProjectConfig() );

                LOGGER.debug( "Returning an in-memory ingest result, no gridded metadata ingest required." );
                return List.of( new IngestResultInMemory( this.getDataSource() ) );
            }

            // Gridded ingest involves ingesting the source only
            return this.getTimeSeriesIngester()
                       .ingest( Stream.of(), this.getDataSource() );
        }
        else
        {
            List<String> variableNames = source.getVariables()
                                               .stream()
                                               .map( Variable::getShortName )
                                               .collect( Collectors.toList() );
            
            throw new PreIngestException( "The NetCDF file at '" +
                                          this.getFileName()
                                          + "' did not contain the "
                                          + "requested variable, "
                                          + variableName
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

        DataSourceConfig dataSourceConfig = this.getDataSource()
                                                .getContext();
        
        if ( Objects.nonNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            timeScale = TimeScaleOuter.of( dataSourceConfig.getExistingTimeScale() );
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
                                                  ConfigHelper.isForecast( dataSourceConfig ),
                                                  timeScale );

        // Acquire the response and ingest the time-series
        SingleValuedTimeSeriesResponse response = Fetcher.getSingleValuedTimeSeries( request );
        for ( Stream<TimeSeries<Double>> nextStream : response.getTimeSeries().values() )
        {
            nextStream.forEach( timeSeries -> this.getTimeSeriesIngester()
                                                  .ingest( Stream.of( TimeSeriesTuple.ofSingleValued( timeSeries, 
                                                                                                      this.getDataSource() ) ),
                                                           this.getDataSource() ) );
        }
    }
}
