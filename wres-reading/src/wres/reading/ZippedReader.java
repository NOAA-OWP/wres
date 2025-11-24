package wres.reading;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.reading.DataSource.DataDisposition;

/**
 * Reads from a zipped source or stream.
 * @author James Brown
 */

public class ZippedReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ZippedReader.class );

    /** The readers for the zipped data. */
    private final TimeSeriesReaderFactory readerFactory;

    /** Name of the inner source, if available. */
    private String innerSourceName;

    /**
     * @param readerFactory a reader factory to help read the archived data
     * @return an instance
     * @throws NullPointerException if the readerFactory is null
     */

    public static ZippedReader of( TimeSeriesReaderFactory readerFactory )
    {
        return new ZippedReader( readerFactory );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource, false );

        Path path = Paths.get( dataSource.getUri() );

        try
        {
            // Open a resource, closed on stream close below
            InputStream fileStream = Files.newInputStream( path );
            return this.read( dataSource, fileStream );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a gzipped source.", e );
        }
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.GZIP );

        try
        {
            GzipCompressorInputStream decompressedStream = new GzipCompressorInputStream( inputStream );
            BufferedInputStream bufferedStream = new BufferedInputStream( decompressedStream );

            // Set the inner source name for identification
            this.innerSourceName = decompressedStream.getMetaData()
                                                     .getFileName();

            LOGGER.debug( "Discovered a source inside {} called {}.", dataSource.getUri(), this.innerSourceName );

            // Determine the content type of the decompressed source and prepare an adapted source that qualifies it
            URI mashupUri = URI.create( dataSource.getUri() + "/" + this.getInnerSourceName() );
            DataDisposition disposition = DataSource.detectFormat( bufferedStream,
                                                                   mashupUri );

            LOGGER.debug( "Detected a decompressed source inside {} with content type {}.",
                          dataSource.getUri(),
                          disposition );

            DataSource decompressedSource = dataSource.toBuilder()
                                                      .uri( mashupUri )
                                                      .disposition( disposition )
                                                      .build();

            TimeSeriesReader reader = this.getReaderFactory()
                                          .getReader( decompressedSource );

            Stream<TimeSeriesTuple> seriesStream = reader.read( decompressedSource, bufferedStream );

            // Close all resources on stream close
            return seriesStream.onClose( () -> {
                try
                {
                    bufferedStream.close();
                    decompressedStream.close();
                    seriesStream.close(); // Invoke the inner close handlers, if any
                }
                catch ( IOException e )
                {
                    LOGGER.warn( "While attempting to close a zipped reader, failed to close one or more dependent "
                                 + "resources." );
                }
            } );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a gzipped source.", e );
        }
    }

    /**
     * @return the reader factory
     */

    private TimeSeriesReaderFactory getReaderFactory()
    {
        return this.readerFactory;
    }

    /**
     * @return the inner source name
     */

    private String getInnerSourceName()
    {
        return this.innerSourceName;
    }

    /**
     * Hidden constructor.
     * @param readerFactory the reader factory, required
     * @throws NullPointerException if the readerFactory is null
     */

    private ZippedReader( TimeSeriesReaderFactory readerFactory )
    {
        Objects.requireNonNull( readerFactory );
        this.readerFactory = readerFactory;
    }

}
