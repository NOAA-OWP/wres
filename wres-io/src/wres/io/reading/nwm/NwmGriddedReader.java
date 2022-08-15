package wres.io.reading.nwm;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import wres.config.generated.Circle;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DataSourceConfig.Variable;
import wres.config.generated.DateCondition;
import wres.config.generated.IntBoundsType;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.UnnamedFeature;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesTuple;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.GriddedFeatures;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.DataSource.DataDisposition;
import wres.io.retrieval.DataAccessException;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;

/**
 * A reader of time-series data from a gridded NetCDF source of National Water Model (NWM) forecasts, simulations or 
 * analyses.
 * 
 * <p>Implementation notes:
 * 
 * This reader currently relies on the gridded reading API, notably {@link Fetcher}, which does not allow for a 
 * streamed input. Thus, {@link #read(DataSource, InputStream)} is a facade on {@link #read(DataSource)}. 
 * 
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class NwmGriddedReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwmGriddedReader.class );

    /** Used to lock gridded features. */
    private static final Object FEATURE_GUARD = new Object();

    /** Thr gridded features, which are cleaned when a new pair declaration appears (based on an identity equals 
     * comparison with an existing pair declaration, since there is one source of pair declaration per evaluation. */
    @GuardedBy( "FEATURE_GUARD" )
    private static final Set<FeatureKey> GRIDDED_FEATURES = new HashSet<>();

    /** Used to determine when a new source of pair declaration appears and hence when the gridded features must be 
     * read. */
    private static final AtomicReference<PairConfig> FEATURE_REFERENCE = new AtomicReference<>();

    /** The maximum number of attempts to open an netcdf source, which may be a remote source, but not served via a web 
     * service API. */
    private static final int MAXIMUM_READ_ATTEMPTS = 5;

    /** Pair declaration. */
    private final PairConfig pairConfig;

    /**
     * @param pairConfig the pair declaration, which is used to determine whether gridded features should be read
     * @return an instance
     * @throws NullPointerException if the pairConfig is null
     */

    public static NwmGriddedReader of( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig );

        return new NwmGriddedReader( pairConfig );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        int tryCount = 0;

        Stream<TimeSeriesTuple> seriesStream;

        while ( true )
        {
            try ( NetcdfFile source = NetcdfFiles.open( dataSource.getUri()
                                                                  .toString() ) )
            {
                seriesStream = this.readNetcdf( dataSource, source );
                break;
            }
            catch ( IOException exception )
            {
                if ( exception.getCause() instanceof SocketTimeoutException &&
                     tryCount < MAXIMUM_READ_ATTEMPTS )
                {
                    LOGGER.error( "Failed to obtain the NetCDF data source from {} after {} failed attempts.",
                                  dataSource.getUri(),
                                  MAXIMUM_READ_ATTEMPTS );
                    tryCount++;
                }
                else
                {
                    throw new ReadException( "Failed to read a gridded NWM source.", exception );
                }
            }
        }

        return seriesStream;
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream stream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( stream );

        LOGGER.warn( "Streaming of NetCDF gridded time-series data is not currently supported. Attempting to read "
                     + "directly from the data source supplied, {}.",
                     dataSource );

        return this.read( dataSource );
    }

    /**
     * @return the pair declaration
     */
    private PairConfig getPairConfig()
    {
        return this.pairConfig;
    }

    /**
     * Reads from a NetCDF file source.
     * @param dataSource the data source
     * @param file the NetCDF source
     * @return the time-series
     * @throws IOException if reading fails
     */

    private Stream<TimeSeriesTuple> readNetcdf( DataSource dataSource, NetcdfFile file ) throws IOException
    {
        // Set the gridded features, if required
        this.setGriddedFeaturesIfRequired( file, this.getPairConfig() );

        // Prepare the grid data request
        Path path = Paths.get( dataSource.getUri() );
        String pathString = path.toString();
        TimeScaleOuter timeScale = null;

        DataSourceConfig dataSourceConfig = dataSource.getContext();

        if ( Objects.nonNull( dataSourceConfig.getExistingTimeScale() ) )
        {
            timeScale = TimeScaleOuter.of( dataSourceConfig.getExistingTimeScale() );
        }

        // Time window constrained only by the pair declaration
        TimeWindowOuter timeWindow = TimeWindowGenerator.getOneBigTimeWindow( this.getPairConfig() );

        Request request = Fetcher.prepareRequest( List.of( pathString ),
                                                  GRIDDED_FEATURES,
                                                  dataSource.getVariable()
                                                            .getValue(),
                                                  timeWindow,
                                                  ConfigHelper.isForecast( dataSourceConfig ),
                                                  timeScale );

        // Acquire the response and ingest the time-series
        SingleValuedTimeSeriesResponse response = Fetcher.getSingleValuedTimeSeries( request );

        Map<FeatureKey, Stream<TimeSeries<Double>>> timeSeries = response.getTimeSeries();

        // Concatenate the per-feature streams
        return timeSeries.values()
                         .stream()
                         .flatMap( s -> s )
                         .map( TimeSeriesTuple::ofSingleValued );
    }

    /**
     * Sets the gridded features if the pair declaration has not been seen before.
     * 
     * @param file the file with gridded features to read
     * @param pairConfig the pair declaration to test
     */

    private void setGriddedFeaturesIfRequired( NetcdfFile file, PairConfig pairConfig )
    {
        // The gridded features are read only once per evaluation under the assumption that every source has the 
        // same grid specification. When a new project arrives, the cache is cleared. This is required for performance. 
        // The following shenanigans implement that goal.
        synchronized ( FEATURE_GUARD )
        {
            // Clear existing features when a new project arrives
            PairConfig oldValue = FEATURE_REFERENCE.getAndSet( pairConfig );
            if ( Objects.nonNull( oldValue ) && oldValue != pairConfig )
            {
                LOGGER.debug( "Clearing gridded features cache because a new project arrived." );

                FEATURE_REFERENCE.set( pairConfig );
                GRIDDED_FEATURES.clear();
            }

            // Gridded features needed?
            if ( GRIDDED_FEATURES.isEmpty() )
            {
                List<UnnamedFeature> gridSelection = this.getPairConfig()
                                                         .getGridSelection();

                try
                {
                    GriddedFeatures features = new GriddedFeatures.Builder( gridSelection ).addFeatures( file )
                                                                                           .build();

                    GRIDDED_FEATURES.addAll( features.get() );

                    LOGGER.debug( "Identified these gridded features whose time-series data should be read: {}.",
                                  features.get() );
                }
                catch ( IOException e )
                {
                    throw new DataAccessException( "Unable to determine the gridded features to read." );
                }
            }
        }
    }

    /**
     * Hidden constructor.
     * @param pairConfig the required pair declaration, which is used to determine when gridded feature ingest is needed
     */

    private NwmGriddedReader( PairConfig pairConfig )
    {
        Objects.requireNonNull( pairConfig );

        this.pairConfig = pairConfig;
    }


    public static void main( String[] args )
    {
        Path path =
                Paths.get( "D:\\Applications\\WRES\\Code\\wres\\systests\\dist\\data\\griddedExamples\\precip_ellicott_city\\short_range\\nwm.20180526",
                           "nwm.t2018052600z.short_range.forcing.f005.wres.nc" );

        DataSourceConfig.Source fakeDeclarationSource =
                new DataSourceConfig.Source( path.toUri(),
                                             InterfaceShortHand.NWM_SHORT_RANGE_CHANNEL_RT_CONUS,
                                             null,
                                             null,
                                             null );

        DataSource fakeSource = DataSource.of( DataDisposition.JSON_WRDS_AHPS,
                                               fakeDeclarationSource,
                                               new DataSourceConfig( null,
                                                                     List.of( fakeDeclarationSource ),
                                                                     new Variable( "RAINRATE", null ),
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null,
                                                                     null ),
                                               Collections.emptyList(),
                                               // Use a fake URI with an NWIS-like string as this is used to trigger the 
                                               // identification of an instantaneous time-scale 
                                               path.toUri(),
                                               LeftOrRightOrBaseline.RIGHT );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                List.of( new UnnamedFeature( null,
                                                                             null,
                                                                             new Circle( 39.225F,
                                                                                         -76.825F,
                                                                                         0.05F,
                                                                                         BigInteger.valueOf( 2346 ) ) ) ),
                                                new IntBoundsType( 1, 18 ),
                                                null,
                                                new DateCondition( "2018-01-01T00:00:00Z", "2021-01-01T00:00:00Z" ),
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        NwmGriddedReader reader = NwmGriddedReader.of( pairConfig );

        System.out.println( reader.read( fakeSource )
                                  .map( next -> next.getSingleValuedTimeSeries() )
                                  .collect( Collectors.toList() ) );

    }


}
