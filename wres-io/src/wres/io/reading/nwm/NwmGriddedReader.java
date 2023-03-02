package wres.io.reading.nwm;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ucar.nc2.NetcdfFile;
import ucar.nc2.NetcdfFiles;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.PairConfig;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.generators.TimeWindowGenerator;
import wres.io.config.ConfigHelper;
import wres.io.database.caching.GriddedFeatures;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.grid.client.Fetcher;
import wres.grid.client.Request;
import wres.grid.client.SingleValuedTimeSeriesResponse;

/**
 * A reader of time-series data from a gridded Netcdf source of National Water Model (NWM) forecasts, simulations or
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

    /** The maximum number of attempts to open an netcdf source, which may be a remote source, but not served via a web 
     * service API. */
    private static final int MAXIMUM_READ_ATTEMPTS = 5;

    /** Pair declaration. */
    private final PairConfig pairConfig;

    /** The gridded features cache. */
    private final GriddedFeatures.Builder features;

    /**
     * @param pairConfig the pair declaration, which is used to determine whether gridded features should be read
     * @param features the gridded features cache
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    public static NwmGriddedReader of( PairConfig pairConfig, GriddedFeatures.Builder features )
    {
        return new NwmGriddedReader( pairConfig, features );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.NETCDF_GRIDDED );

        // Validate that the source contains a readable file or directory
        ReaderUtilities.validateFileSource( dataSource, true );

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
                    LOGGER.error( "Failed to obtain the Netcdf data source from {} after {} failed attempts.",
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

        LOGGER.warn( "Streaming of Netcdf gridded time-series data is not currently supported. Attempting to read "
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
     * Reads from a Netcdf file source.
     * @param dataSource the data source
     * @param file the Netcdf source
     * @return the time-series
     * @throws IOException if reading fails due to an underlying error in the netcdf reading
     * @throws ReadException if the gridded features are not available at read time
     */

    private Stream<TimeSeriesTuple> readNetcdf( DataSource dataSource, NetcdfFile file ) throws IOException
    {
        GriddedFeatures griddedFeatures = this.getGriddedFeatures()
                                              .build();

        Set<Feature> featureKeys = griddedFeatures.get();

        // Gridded features must be set now
        if ( featureKeys.isEmpty() )
        {
            throw new ReadException( "While preparing to read "
                                     + file.getLocation()
                                     + ", discovered an empty cache of gridded features. Cannot read a gridded dataset "
                                     + "without a cache of gridded features." );
        }

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
                                                  featureKeys,
                                                  dataSource.getVariable()
                                                            .getValue(),
                                                  timeWindow,
                                                  ConfigHelper.isForecast( dataSourceConfig ),
                                                  timeScale );

        // Acquire the response and return the time-series
        SingleValuedTimeSeriesResponse response = Fetcher.getSingleValuedTimeSeries( request );

        Map<Feature, Stream<TimeSeries<Double>>> timeSeries = response.getTimeSeries();

        // Concatenate the per-feature streams
        return timeSeries.values()
                         .stream()
                         .flatMap( s -> s )
                         .map( next -> ReaderUtilities.validateAgainstEmptyTimeSeries( next, dataSource.getUri() ) )
                         .map( next -> TimeSeriesTuple.ofSingleValued( next, dataSource ) );
    }

    /**
     * @return the features cache
     */

    private GriddedFeatures.Builder getGriddedFeatures()
    {
        return this.features;
    }

    /**
     * Hidden constructor.
     * @param pairConfig the required pair declaration, which is used to determine when gridded feature ingest is needed
     * @param features the gridded features cache
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the features cache does not include any gridded features
     */

    private NwmGriddedReader( PairConfig pairConfig, GriddedFeatures.Builder features )
    {
        Objects.requireNonNull( pairConfig );
        Objects.requireNonNull( features );

        this.pairConfig = pairConfig;
        this.features = features;
    }

}
