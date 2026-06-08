package wres.reading.parquet;

import java.io.InputStream;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.reading.DataSource;
import wres.reading.DataSource.DataDisposition;
import wres.reading.ReadException;
import wres.reading.ReaderUtilities;
import wres.reading.TimeSeriesReader;
import wres.reading.TimeSeriesTuple;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;

/**
 * <p>Reads a Parquet file with a simple format that contains a single time-series for a single geographic feature and
 * variable, as exemplified below:
 *
 * <p>value_time value
 * 2008-10-01 00:00:00 4.587329
 * 2008-10-01 00:15:00 4.587329
 *
 * <p>Optionally, the Parquet file may contain the following global metadata key-value pairs:
 *
 * <ol>
 *     <li>feature_id: the identifier of the geographic feature (e.g., "50147800")</li>
 *     <li>variable_name: the variable name (e.g., "streamflow")</li>
 *     <li>measurement_unit: the measurement unit (e.g., m3/s)</li>
 * </ol>
 *
 * @author James Brown
 */

public class ParquetReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ParquetReader.class );

    /** Default value string. */
    private static final String UNKNOWN = "unknown";

    /** Database URL> **/
    private static final String DUCKDB_JDBC_URL = "jdbc:jfr:duckdb:";

    /**
     * @return an instance
     */

    public static ParquetReader of()
    {
        return new ParquetReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.PARQUET );

        return this.readInner( dataSource );
    }

    /**
     * Eager method that consumes all bytes in the stream, since the Parquet format does not support sequential/stream
     * reading. This method is useful for testing without a file system, as it uses the same pathway for format reading
     * as {@link #read(DataSource)}, which is the main entrypoint.
     *
     * @see #read(DataSource)
     * @param dataSource the data source, required
     * @param inputStream the input stream, required
     * @return the input stream
     */

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        // Validate the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.PARQUET );

        LOGGER.warn( "The Parquet format is a random access format for which sequential streaming is not supported. "
                     + "Attempting to read directly from the data source supplied, {}.",
                     dataSource );

        return this.read( dataSource );
    }

    /**
     * @param dataSource the data source
     * @return the time-series streams
     * @throws ReadException if the data source could not be read
     * @throws NullPointerException if either input is null
     */

    private Stream<TimeSeriesTuple> readInner( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Get the lazy supplier of time-series data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, new AtomicBoolean() );

        // Generate a stream of time-series. Nothing is read here. Rather, as part of a terminal operation on this
        // stream, each pull will read through to the supplier, then in turn to the data provider, and finally to
        // the data source.
        return Stream.generate( supplier )
                     // Take only while series are returned, stop when the null sentinel is received
                     .takeWhile( Objects::nonNull )
                     // Close the data provider when the stream is closed
                     .onClose( () -> LOGGER.debug( "Detected a stream close event." ) );
    }

    /**
     * Returns a time-series supplier from the inputs.
     *
     * @param dataSource the data source
     * @param readStatus whether the file has been read before
     * @return a time-series supplier
     * @throws ReadException if the file could not be read for any reason
     * @throws NullPointerException if either input is null
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             AtomicBoolean readStatus )
    {
        Objects.requireNonNull( dataSource );

        return () ->
        {
            // Return null sentinel to stop reading more data as there is only one series
            if ( readStatus.getAndSet( true ) )
            {
                return null;
            }

            TimeSeriesMetadata metadata = this.getTimeSeriesMetadata( dataSource );

            TimeSeries.Builder<Double> timeSeriesBuilder = new TimeSeries.Builder<Double>()
                    .setMetadata( metadata );
            String targetFilePath = this.getParquetPath( dataSource );
            String querySql = String.format( "SELECT value_time, sim_flow FROM read_parquet('%s')", targetFilePath );

            try ( Connection conn = DriverManager.getConnection( DUCKDB_JDBC_URL );
                  Statement stmt = conn.createStatement();
                  ResultSet rs = stmt.executeQuery( querySql ) )
            {
                while ( rs.next() )
                {
                    Timestamp timestampVal = rs.getTimestamp( "value_time" );
                    LocalDateTime localDateTime = timestampVal.toLocalDateTime();

                    // A fairy story, but the format does not provide sufficient information for any other assumption
                    // because adjustedToUTC=false.
                    Instant timestamp = localDateTime.toInstant( ZoneOffset.UTC );

                    double rawFlow = rs.getDouble( "sim_flow" );
                    timeSeriesBuilder.addEvent( Event.of( timestamp, rawFlow ) );
                }
            }
            catch ( SQLException e )
            {
                throw new ReadException(
                        "Could not read the time-series from a Parquet file contained in this data source:"
                        + dataSource,
                        e );
            }

            TimeSeries<Double> timeSeries = timeSeriesBuilder.build();

            return TimeSeriesTuple.ofSingleValued( timeSeries, dataSource );
        };
    }

    /**
     * Reads the time-series metadata from the source file.
     * @param dataSource the data source
     * @return the time-series metadata
     * @throws ReadException if the metadata could not be read for any reason
     */

    private TimeSeriesMetadata getTimeSeriesMetadata( DataSource dataSource )
    {
        String featureId = UNKNOWN;
        String variableName = UNKNOWN;
        String measurementUnit = UNKNOWN;
        String targetFilePath = this.getParquetPath( dataSource );
        String metadataSql = String.format( "SELECT CAST(key AS VARCHAR) AS key_str, CAST(value AS VARCHAR) AS val_str "
                                            + "FROM parquet_kv_metadata('%s')", targetFilePath );

        try ( Connection conn = DriverManager.getConnection( DUCKDB_JDBC_URL );
              Statement stmt = conn.createStatement();
              ResultSet rs = stmt.executeQuery( metadataSql ) )
        {
            while ( rs.next() )
            {
                String key = rs.getString( "key_str" );
                String value = rs.getString( "val_str" );
                if ( "feature_id".equalsIgnoreCase( key ) )
                {
                    featureId = value;
                }
                else if ( "variable_name".equalsIgnoreCase( key ) )
                {
                    variableName = value;
                }
                else if ( "measurement_unit".equalsIgnoreCase( key ) )
                {
                    measurementUnit = value;
                }
            }
        }
        catch ( SQLException e )
        {
            throw new ReadException( "Failed to read the time-series metadata for a data source with URI: "
                                     + dataSource.uri(), e );
        }

        Geometry geometry = MessageUtilities.getGeometry( featureId );
        Feature feature = Feature.of( geometry );

        return new TimeSeriesMetadata.Builder()
                .setReferenceTimes( Map.of() )
                .setFeature( feature )
                .setVariableName( variableName )
                .setUnit( measurementUnit )
                .build();
    }

    /**
     * @param dataSource the data source
     * @return the path tpo the Parquet file
     */

    private String getParquetPath( DataSource dataSource )
    {
        return Paths.get( dataSource.uri() )
                    .toAbsolutePath()
                    .toString()
                    .replace( "\\", "/" );
    }

    /**
     * Constructor.
     */

    private ParquetReader()
    {
        // Do not construct
    }
}

