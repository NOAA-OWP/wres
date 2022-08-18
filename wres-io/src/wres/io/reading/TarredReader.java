package wres.io.reading;

import static wres.io.reading.DataSource.DataDisposition.UNKNOWN;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.datamodel.time.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;

/**
 * Reads from a tarred source or stream.
 * @author James Brown
 */

public class TarredReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TarredReader.class );

    /** The readers for the archived data. */
    private final TimeSeriesReaderFactory readerFactory;

    /**
     * @param readerFactory a reader factory to help read the archived data
     * @return an instance
     * @throws NullPointerException if the readerFactory is null
     */

    public static TarredReader of( TimeSeriesReaderFactory readerFactory )
    {
        return new TarredReader( readerFactory );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );
        
        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource );
        
        try
        {
            Path path = Paths.get( dataSource.getUri() );
            InputStream stream = new BufferedInputStream( Files.newInputStream( path ) );
            return this.read( dataSource, stream );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a tarred source.", e );
        }
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.TARBALL );

        // Get the lazy supplier of time-series data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, inputStream );

        // Generate a stream of time-series.
        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     // Close the data provider when the stream is closed
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event, closing an underlying data provider." );

                         try
                         {
                             inputStream.close();
                         }
                         catch ( IOException e )
                         {
                             LOGGER.warn( "Unable to close a stream for data source {}.",
                                          dataSource.getUri() );
                         }
                     } );
    }

    /**
     * Returns a time-series supplier from the inputs.
     * 
     * @param dataSource the data source
     * @param inputStream the stream to read
     * @return a time-series supplier
     * @throws ReadException if the data could not be read for any reason
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource,
                                                             InputStream inputStream )
    {
        AtomicReference<Iterator<TimeSeriesTuple>> lastTuples = new AtomicReference<>();
        TarArchiveInputStream archiveStream = new TarArchiveInputStream( inputStream );

        // Create a supplier that returns a time-series once complete
        return () -> {

            // Any tuples from the last pull to return still?
            Iterator<TimeSeriesTuple> nextTuples = lastTuples.get();

            if ( Objects.nonNull( nextTuples ) && nextTuples.hasNext() )
            {
                // Remove and return the next one
                return nextTuples.next();
            }

            try
            {
                TarArchiveEntry archivedSource = archiveStream.getNextTarEntry();

                // While there are still archive entries left
                while ( Objects.nonNull( archivedSource ) )
                {
                    // Find the next one that is a file
                    if ( archivedSource.isFile() )
                    {
                        Stream<TimeSeriesTuple> stream = this.readTarEntry( dataSource,
                                                                            archivedSource,
                                                                            archiveStream,
                                                                            dataSource.getUri() );

                        // Find the next one that produced one or more time-series
                        if ( Objects.nonNull( stream ) )
                        {
                            // Get an iterator for the series
                            Iterator<TimeSeriesTuple> newTuples = stream.iterator();
                            lastTuples.set( newTuples );

                            // Find the next one that actually has a time-series and is not an empty file, for example
                            if ( newTuples.hasNext() )
                            {
                                return newTuples.next();
                            }
                        }
                    }

                    // Try again until we find the next file with one or more time-series or reach the end
                    archivedSource = archiveStream.getNextTarEntry();
                }

                // Null sentinel to close the stream
                return null;

            }
            catch ( IOException e )
            {
                throw new ReadException( "While processing a tarred archive.", e );
            }
        };
    }

    /**
     * @param dataSource the data source
     * @param archiveEntry the archive entry
     * @param archiveInputStream the archive input stream
     * @param tarName the named of the tar entry
     * @return a stream of time-series
     */

    private Stream<TimeSeriesTuple> readTarEntry( DataSource dataSource,
                                                  TarArchiveEntry archiveEntry,
                                                  InputStream archiveInputStream,
                                                  URI tarName )
    {
        int expectedByteCount = (int) archiveEntry.getSize();
        URI archivedFileName = URI.create( tarName + File.pathSeparator + archiveEntry.getName() );

        byte[] content = new byte[expectedByteCount];

        try
        {
            archiveInputStream.read( content, 0, content.length );
        }
        catch ( EOFException eof )
        {
            String message = "The end of the archive entry for: '"
                             + archivedFileName
                             + "' was "
                             + "reached. Data within the archive may have been "
                             + "cut off when creating or moving the archive.";
            // Regarding whether to log-and-continue or to propagate and stop:
            // On the one hand, it might happen on a file that is intended for
            // ingest and therefore can affect primary outputs if not ingested.
            // On the other hand, this is a very specific case of a particular
            // exception when reading a particular file within an archive.
            // But then again, shouldn't the user be notified that the archive
            // is corrupt? And if so, stopping (propagating here) is the
            // clearest way to notify the user of a corrupt input file.
            throw new ReadException( message, eof );
        }
        catch ( IOException ioe )
        {
            throw new ReadException( "Failed to read from '"
                                     + archivedFileName
                                     + "'",
                                     ioe );
        }

        // Detect the data disposition
        DataDisposition disposition = null;
        try ( InputStream detect = new ByteArrayInputStream( content ) )
        {
            disposition = DataSource.detectFormat( detect, archivedFileName );
        }
        catch ( IOException e )
        {
            String message =
                    "Encountered an error while attempting to close a stream used for content type detection for "
                             + archivedFileName
                             + ".";
            LOGGER.warn( message, e );
        }

        if ( disposition == UNKNOWN )
        {
            LOGGER.warn( "Skipping unknown data type in {}.", archivedFileName );

            return Stream.of();
        }

        DataSourceConfig.Source originalSource = dataSource.getSource();

        if ( Objects.isNull( originalSource ) )
        {
            // Demote to debug or trace if null is known as being a normal,
            // usual occurrence that has no potential impact on anything.
            LOGGER.warn( "Archive entry '{}' is not being read because its data source is null.",
                         archiveEntry );

            return Stream.of();
        }

        LOGGER.debug( "The file '{}' will now be read.", archivedFileName );

        // Create the inner data source and stream
        DataSource innerDataSource = DataSource.of( disposition,
                                                    originalSource,
                                                    dataSource.getContext(),
                                                    dataSource.getLinks(),
                                                    archivedFileName,
                                                    dataSource.getLeftOrRightOrBaseline() );

        InputStream streamToRead = new ByteArrayInputStream( content );

        TimeSeriesReader reader = this.getReaderFactory()
                                      .getReader( innerDataSource );

        return reader.read( innerDataSource, streamToRead );
    }

    /**
    * @return the reader factory
    */

    private TimeSeriesReaderFactory getReaderFactory()
    {
        return this.readerFactory;
    }

    /**
     * Hidden constructor.
     * @param readerFactory the reader factory, required
     * @throws NullPointerException if the readerFactory is null
     */

    private TarredReader( TimeSeriesReaderFactory readerFactory )
    {
        Objects.requireNonNull( readerFactory );
        this.readerFactory = readerFactory;
    }
}
