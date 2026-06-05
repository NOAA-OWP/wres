package wres.reading.parquet;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.jetbrains.annotations.NotNull;
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
        this.validateDataSource( dataSource );

        Path path = Paths.get( dataSource.uri() );

        InputFile inputFile = new LocalInputFile( path );
        return this.read( dataSource, inputFile );
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
        this.validateDataSource( dataSource );

        try
        {
            byte[] bytes = inputStream.readAllBytes();
            InputFile inputFile = new ByteArrayInputFile( bytes );
            return this.read( dataSource, inputFile );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read from an input stream for this data source: " + dataSource );
        }
    }

    /**
     * @param parquetFile the parquet file
     * @param dataSource the data source
     * @return the time-series streams
     * @throws ReadException if the data source could not be read
     * @throws NullPointerException if either input is null
     */

    private Stream<TimeSeriesTuple> read( DataSource dataSource,
                                          InputFile parquetFile )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( parquetFile );

        // Get the lazy supplier of time-series data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, parquetFile, new AtomicBoolean() );

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
     * @param parquetFile the parquet file
     * @param readStatus whether the file has been read before
     * @return a time-series supplier
     * @throws ReadException if the file could not be read for any reason
     * @throws NullPointerException if either input is null
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             InputFile parquetFile,
                                                             AtomicBoolean readStatus )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( parquetFile );

        return () ->
        {
            // Return null sentinel to stop reading more data as there is only one series
            if ( readStatus.getAndSet( true ) )
            {
                return null;
            }

            ParquetConfiguration conf = new PlainParquetConfiguration();

            TimeSeriesMetadata metadata = this.getTimeSeriesMetadata( dataSource,
                                                                      parquetFile );

            TimeSeries.Builder<Double> timeSeriesBuilder = new TimeSeries.Builder<Double>()
                    .setMetadata( metadata );

            int timeIdx = -1;
            int flowIdx = -1;
            boolean indicesResolved = false;

            try ( org.apache.parquet.hadoop.ParquetReader<Group> reader = new HadoopFreeParquetBuilder( parquetFile )
                    .withConf( conf )
                    .build() )
            {

                Group group;
                while ( ( group = reader.read() ) != null )
                {
                    if ( !indicesResolved )
                    {
                        timeIdx = group.getType()
                                       .getFieldIndex( "value_time" );
                        flowIdx = group.getType()
                                       .getFieldIndex( "sim_flow" );
                        indicesResolved = true;
                    }

                    long rawEpochTime = group.getLong( timeIdx, 0 );
                    long seconds = rawEpochTime / 1_000_000_000L;
                    int nanos = ( int ) ( rawEpochTime % 1_000_000_000L );

                    // Create a local time as adjustedToUTC is false
                    LocalDateTime localDateTime = LocalDateTime.ofEpochSecond( seconds, nanos, ZoneOffset.UTC );

                    double rawFlow = group.getFloat( flowIdx, 0 );

                    // But assume UTC anyway for now
                    Instant timestamp = localDateTime.toInstant( ZoneOffset.UTC );
                    timeSeriesBuilder.addEvent( Event.of( timestamp, rawFlow ) );
                }
            }
            catch ( IOException e )
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
     * @param parquetFile the parquet file
     * @return the time-series metadata
     * @throws ReadException if the metadata could not be read for any reason
     */

    private TimeSeriesMetadata getTimeSeriesMetadata( DataSource dataSource,
                                                      InputFile parquetFile )
    {
        Map<String, String> fileMetadata;
        try ( ParquetFileReader metaReader = ParquetFileReader.open( parquetFile ) )
        {
            fileMetadata = metaReader.getFileMetaData().getKeyValueMetaData();
        }
        catch ( IOException e )
        {
            throw new ReadException( "Could not read the metadata from a Parquet file contained in this data source:"
                                     + dataSource,
                                     e );
        }

        TimeSeriesMetadata.Builder metadata = new TimeSeriesMetadata.Builder();

        String featureId = ( fileMetadata != null ) ?
                fileMetadata.getOrDefault( "feature_id", UNKNOWN ) : UNKNOWN;

        metadata.setFeature( Feature.of( Geometry.newBuilder()
                                                 .setName( featureId )
                                                 .build() ) );

        String variableName = ( fileMetadata != null ) ?
                fileMetadata.getOrDefault( "variable_name", UNKNOWN ) : UNKNOWN;

        metadata.setVariableName( variableName );

        String unit = ( fileMetadata != null ) ?
                fileMetadata.getOrDefault( "measurement_unit", UNKNOWN ) : UNKNOWN;

        metadata.setUnit( unit );

        metadata.setReferenceTimes( Map.of() );

        return metadata.build();
    }

    /**
     * Validates the data source.
     *
     * @param dataSource the data source
     */

    private void validateDataSource( DataSource dataSource )
    {
        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.PARQUET );

        if ( Objects.nonNull( dataSource.source()
                                        .timeZoneOffset() )
             && !Objects.equals( dataSource.source()
                                           .timeZoneOffset(),
                                 ZoneOffset.UTC ) )
        {
            throw new ReadException( "The declared 'time_zone_offset' for the data source at '"
                                     + dataSource.uri()
                                     + "' was '"
                                     + dataSource.source()
                                                 .timeZoneOffset()
                                     + "', which is inconsistent with the CSV format requirement that all times are "
                                     + "supplied in UTC. Please resolve this conflict and try again." );
        }
    }

    /**
     * Constructor.
     */

    private ParquetReader()
    {
        // Do not construct
    }

    /**
     * A convenience class the provides a file interface to a byte stream for reading.
     */

    private static class ByteArrayInputFile implements InputFile
    {
        private final byte[] data;

        public ByteArrayInputFile( byte[] data )
        {
            this.data = data;
        }

        @Override
        public long getLength()
        {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream()
        {
            return new SeekableByteArrayInputStream( data );
        }

        private static class SeekableByteArrayInputStream extends SeekableInputStream
        {
            private final byte[] data;
            private int pos = 0;

            public SeekableByteArrayInputStream( byte[] data )
            {
                this.data = data;
            }

            @Override
            public long getPos()
            {
                return pos;
            }

            @Override
            public void seek( long newPos ) throws IOException
            {
                if ( newPos < 0 || newPos > data.length )
                {
                    throw new IOException( "Seek out of bounds." );
                }
                this.pos = ( int ) newPos;
            }

            @Override
            public int read()
            {
                return ( pos < data.length ) ? ( data[pos++] & 0xff ) : -1;
            }

            @Override
            public int read( @NotNull byte[] b, int off, int len )
            {
                if ( pos >= data.length )
                {
                    return -1;
                }
                int avail = data.length - pos;
                if ( len > avail )
                {
                    len = avail;
                }
                System.arraycopy( data, pos, b, off, len );
                pos += len;
                return len;
            }

            @Override
            public void readFully( byte[] b ) throws IOException
            {
                readFully( b, 0, b.length );
            }

            @Override
            public void readFully( byte[] b, int off, int len ) throws IOException
            {
                if ( read( b, off, len ) < len )
                {
                    throw new EOFException();
                }
            }

            @Override
            public int read( ByteBuffer buf )
            {
                if ( pos >= data.length )
                {
                    return -1;
                }
                int avail = data.length - pos;
                int toRead = Math.min( buf.remaining(), avail );
                if ( toRead <= 0 )
                {
                    return 0;
                }

                buf.put( data, pos, toRead );
                pos += toRead;
                return toRead;
            }

            @Override
            public void readFully( ByteBuffer buf ) throws IOException
            {
                int expected = buf.remaining();
                if ( expected <= 0 )
                {
                    return;
                }

                int avail = data.length - pos;
                if ( expected > avail )
                {
                    throw new EOFException( "Not enough bytes remaining to fill the supplied buffer." );
                }

                buf.put( data, pos, expected );
                pos += expected;
            }
        }
    }

    /**
     * A Parquet reader that does not depend on a Hadoop file system.
     */

    private static class HadoopFreeParquetBuilder extends org.apache.parquet.hadoop.ParquetReader.Builder<Group>
    {
        protected HadoopFreeParquetBuilder( InputFile file )
        {
            super( file );
        }

        @Override
        protected ReadSupport<Group> getReadSupport()
        {
            return new GroupReadSupport();
        }
    }
}

