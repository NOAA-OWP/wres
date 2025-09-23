package wres.reading.netcdf.nwm;

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

import wres.config.DeclarationUtilities;
import wres.config.components.Dataset;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.TimeWindowSlicer;
import wres.reading.netcdf.grid.GridRequest;
import wres.reading.netcdf.grid.GridReader;
import wres.reading.netcdf.grid.GriddedFeatures;
import wres.reading.DataSource;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.reading.DataSource.DataDisposition;

/**
 * <p>A reader of time-series data from a gridded Netcdf source of National Water Model (NWM) forecasts, simulations or
 * analyses.
 *
 * <p>Implementation notes:
 *
 * <p>This reader currently relies on the gridded reading API, notably {@link GridReader}, which does not allow for a
 * streamed input. Thus, {@link #read(DataSource, InputStream)} is a facade on {@link #read(DataSource)}. 
 *
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class NwmGridReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NwmGridReader.class );

    /** The maximum number of attempts to open an netcdf source, which may be a remote source, but not served via a web 
     * service API. */
    private static final int MAXIMUM_READ_ATTEMPTS = 5;

    /** Declaration. */
    private final EvaluationDeclaration declaration;

    /** The gridded features cache. */
    private final GriddedFeatures.Builder features;

    /**
     * @param declaration the declaration, which is used to determine whether gridded features should be read
     * @param features the gridded features cache
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    public static NwmGridReader of( EvaluationDeclaration declaration, GriddedFeatures.Builder features )
    {
        return new NwmGridReader( declaration, features );
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
    private EvaluationDeclaration getDeclaration()
    {
        return this.declaration;
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

        Dataset dataset = dataSource.getContext();
        wres.config.components.TimeScale declaredTimeScale = dataset.timeScale();

        if ( Objects.nonNull( declaredTimeScale ) )
        {
            timeScale = TimeScaleOuter.of( declaredTimeScale.timeScale() );
        }

        // Time window constrained only by the pair declaration
        TimeWindowOuter timeWindow = TimeWindowSlicer.getOneBigTimeWindow( this.getDeclaration() );

        // The gridded reader requires a hint about the data type, specifically whether it is a forecast type. This is
        // unlike most other readers where the type can be inferred directly from the structure of the time-series data,
        // but all gridded datasets have a reference time regardless of type. When the type is explicitly declared, use
        // that, i.e., inspect the dataset for the type declaration. However, the other declaration can be useful too.
        // This is a bit hairy, but a user can avoid this by explicitly declaring the type.
        // For the route where grids are read at "retrieval time", a similar inspection occurs, informed by the
        // declaration. See #51232 also, which aims to promote this reader pathway in all contexts.
        boolean isForecast = DeclarationUtilities.isForecast( dataset )
                || ( ( dataSource.getDatasetOrientation() == DatasetOrientation.RIGHT
                             || dataSource.getDatasetOrientation() == DatasetOrientation.BASELINE )
                && DeclarationUtilities.hasForecastDeclaration( this.getDeclaration() ) );

        GridRequest request = new GridRequest( List.of( pathString ),
                                               featureKeys,
                                               dataSource.getVariable()
                                                         .name(),
                                               timeWindow,
                                               isForecast,
                                               timeScale );

        // Acquire the time-series
        Map<Feature, Stream<TimeSeries<Double>>> timeSeries = GridReader.getSingleValuedTimeSeries( request );

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
     * @param declaration the declaration, which is used to determine when gridded feature ingest is needed
     * @param features the gridded features cache
     * @throws NullPointerException if either input is null
     * @throws IllegalArgumentException if the features cache does not include any gridded features
     */

    private NwmGridReader( EvaluationDeclaration declaration, GriddedFeatures.Builder features )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( features );

        this.declaration = declaration;
        this.features = features;
    }
}
