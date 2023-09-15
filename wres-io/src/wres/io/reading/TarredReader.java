package wres.io.reading;

import static wres.io.reading.DataSource.DataDisposition.UNKNOWN;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.Source;
import wres.io.reading.DataSource.DataDisposition;
import wres.system.SystemSettings;

/**
 * Reads from a tarred source or stream. Create one reader per source.
 * @author James Brown
 */

public class TarredReader implements TimeSeriesReader
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TarredReader.class );

    /** Path delimiter. */
    private static final String PATH_DELIM = "/";

    /** Error message. */
    private static final String WHILE_PROCESSING_A_TARRED_ARCHIVE = "While processing a tarred archive.";

    /** The readers for the archived data. */
    private final TimeSeriesReaderFactory readerFactory;

    /** A thread pool to read archive entries. */
    private final ThreadPoolExecutor executor;

    /**
     * @param readerFactory a reader factory to help read the archived data
     * @param systemSettings the system settings
     * @return an instance
     * @throws NullPointerException if either input is null
     */

    public static TarredReader of( TimeSeriesReaderFactory readerFactory, SystemSettings systemSettings )
    {
        return new TarredReader( readerFactory, systemSettings );
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource, false );

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
                             this.getExecutor()
                                 .shutdownNow();
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
        AtomicReference<List<TimeSeriesTuple>> lastTuples = new AtomicReference<>();
        Queue<Future<List<TimeSeriesTuple>>> tuples = new LinkedList<>();
        TarArchiveInputStream archiveStream = new TarArchiveInputStream( inputStream );
        CountDownLatch startGettingOnTasks = new CountDownLatch( this.getExecutor()
                                                                     .getMaximumPoolSize() );

        // Create a supplier that returns a time-series once complete
        return () -> {

            // Any tuples from the last pull to return still?
            List<TimeSeriesTuple> nextTuples = lastTuples.get();

            if ( Objects.nonNull( nextTuples ) && !nextTuples.isEmpty() )
            {
                // Remove and return the next one
                return nextTuples.remove( 0 );
            }

            try
            {
                TarArchiveEntry archivedSource = archiveStream.getNextTarEntry();

                // Flag to indicate when the loop should stop
                boolean proceed = true;

                // While there are still archive entries left
                while ( proceed )
                {
                    TimeSeriesTuple tuple = this.getNextTupleOrIncrementCaches( dataSource,
                                                                                archivedSource,
                                                                                archiveStream,
                                                                                lastTuples,
                                                                                tuples,
                                                                                startGettingOnTasks );

                    // Return a tuple if found
                    if ( Objects.nonNull( tuple ) )
                    {
                        return tuple;
                    }

                    // Try again until we find the next entry with one or more time-series or reach the end
                    archivedSource = archiveStream.getNextTarEntry();

                    // Proceed while there is another archive entry, another future awaiting retrieval or another 
                    // retrieved tuple awaiting return 
                    proceed = Objects.nonNull( archivedSource )
                              || ( Objects.nonNull( lastTuples.get() ) && !lastTuples.get()
                                                                                     .isEmpty() )
                              || !tuples.isEmpty();

                    LOGGER.trace( "Archived entry? {}. Last tuples? {}. How many tuples are queued? {}.",
                                  Objects.nonNull( archivedSource ),
                                  ( Objects.nonNull( lastTuples.get() ) && !lastTuples.get()
                                                                                      .isEmpty() ),
                                  tuples.size() );
                }

                // Null sentinel to close the stream
                return null;
            }
            catch ( IOException e )
            {
                throw new ReadException( WHILE_PROCESSING_A_TARRED_ARCHIVE, e );
            }
        };
    }

    /**
     * Returns a time-series or increments the caches to return next time.
     * @param dataSource the data source
     * @param archivedSource the metadata of the archive entry that requires reading
     * @param archiveStream the archive stream from which to read bytes for the next entry
     * @param lastTuples the last tuples from which to obtain a time-series, if possible
     * @param tuples the queue of all future tuples to increment
     * @param startGettingOnTasks a latch indicating when to start getting results
     * @return a time-series or null
     */

    private TimeSeriesTuple getNextTupleOrIncrementCaches( DataSource dataSource,
                                                           TarArchiveEntry archivedSource,
                                                           TarArchiveInputStream archiveStream,
                                                           AtomicReference<List<TimeSeriesTuple>> lastTuples,
                                                           Queue<Future<List<TimeSeriesTuple>>> tuples,
                                                           CountDownLatch startGettingOnTasks )
    {
        // Find the next one that is a file
        if ( Objects.nonNull( archivedSource ) && archivedSource.isFile() )
        {
            // Create the stream, which means reading the bytes but not translating them yet
            Stream<TimeSeriesTuple> stream = this.readTarEntry( dataSource,
                                                                archivedSource,
                                                                archiveStream,
                                                                dataSource.getUri() );

            // Create the next mutable list of tuples, submitting the task to the executor for delayed execution
            Future<List<TimeSeriesTuple>> nextTuple =
                    this.getExecutor()
                        .submit( () -> {

                            // Find the next one that produced one or more time-series
                            if ( Objects.nonNull( stream ) )
                            {
                                // Close the stream on completion
                                try ( stream )
                                {
                                    // Pull/read from the stream, which means translating the bytes
                                    return stream.collect( Collectors.toCollection( ArrayList::new ) );
                                }
                            }

                            return null;
                        } );

            // Tasks submitted to count down
            startGettingOnTasks.countDown();
            tuples.add( nextTuple );
        }

        try
        {
            // Start getting results if the latch has reached zero or the next archived entry is null, i.e., there
            // are fewer tasks than the initial latch count
            if ( startGettingOnTasks.await( 0, TimeUnit.MILLISECONDS ) || Objects.isNull( archivedSource ) )
            {
                Future<List<TimeSeriesTuple>> earlierTask = tuples.poll();

                if ( Objects.nonNull( earlierTask ) )
                {
                    List<TimeSeriesTuple> result = earlierTask.get();

                    // Result to return? Remove from the immediate cache and return it
                    if ( Objects.nonNull( result ) && !result.isEmpty() )
                    {
                        lastTuples.set( result );
                        return result.remove( 0 );
                    }
                }
            }
        }
        catch ( ExecutionException e )
        {
            throw new ReadException( WHILE_PROCESSING_A_TARRED_ARCHIVE, e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread()
                  .interrupt();
            throw new ReadException( WHILE_PROCESSING_A_TARRED_ARCHIVE, e );
        }

        return null;
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
        LOGGER.debug( "Attempting to read a tar entry from {}.", dataSource );

        int expectedByteCount = (int) archiveEntry.getSize();
        URI archivedFileName = URI.create( tarName + PATH_DELIM + archiveEntry.getName() );

        // Detect the data disposition using a markable stream
        InputStream bufferedStream = new BufferedInputStream( archiveInputStream );
        DataDisposition disposition = DataSource.detectFormat( bufferedStream, archivedFileName );

        if ( disposition == UNKNOWN )
        {
            LOGGER.warn( "Skipping unknown data type in {}.", archivedFileName );

            return Stream.of();
        }

        Source originalSource = dataSource.getSource();

        if ( Objects.isNull( originalSource ) )
        {
            // Demote to debug or trace if null is known as being a normal,
            // usual occurrence that has no potential impact on anything.
            LOGGER.warn( "Archive entry '{}' is not being read because its data source is null.",
                         archiveEntry );

            return Stream.of();
        }

        // Create the inner data source and stream
        DataSource innerDataSource = DataSource.of( disposition,
                                                    originalSource,
                                                    dataSource.getContext(),
                                                    dataSource.getLinks(),
                                                    archivedFileName,
                                                    dataSource.getDatasetOrientation() );

        LOGGER.debug( "Created an inner data source from a tarred archive entry: {}.", innerDataSource );

        LOGGER.debug( "The tarred entry '{}' will now be read.", archivedFileName );

        // Read all the bytes: unfortunately, this requires as much memory as the size of the archive entry, but 
        // the parent/archive stream cannot be shared between low-level reader threads, in any case, because it is not 
        // thread safe: #108595. Thus, while it would be possible to wrap the input stream with a byte-limited stream,
        // such as an org.apache.commons.io.input.BoundedInputStream, this would expose the archive stream to async 
        // reads, which would cause interference
        byte[] content = new byte[expectedByteCount];

        try
        {
            int bytesRead = bufferedStream.readNBytes( content, 0, expectedByteCount );
            LOGGER.debug( "Read {} bytes of an expected {} bytes from {}.",
                          bytesRead,
                          expectedByteCount,
                          archivedFileName );
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
     * @return the thread pool executor
     */

    private ThreadPoolExecutor getExecutor()
    {
        return this.executor;
    }

    /**
     * Hidden constructor.
     * @param readerFactory the reader factory, required
     * @param systemSettings the system settings
     * @throws NullPointerException if either input is null
     */

    private TarredReader( TimeSeriesReaderFactory readerFactory, SystemSettings systemSettings )
    {
        Objects.requireNonNull( readerFactory );
        Objects.requireNonNull( systemSettings );

        this.readerFactory = readerFactory;

        ThreadFactory tarredSourceFactory = new BasicThreadFactory.Builder().namingPattern( "Tarred Reading Thread %d" )
                                                                            .build();
        BlockingQueue<Runnable> tarredSourceQueue = new ArrayBlockingQueue<>( systemSettings.getMaximumArchiveThreads() );
        this.executor = new ThreadPoolExecutor( systemSettings.getMaximumArchiveThreads(),
                                                systemSettings.getMaximumArchiveThreads(),
                                                systemSettings.getPoolObjectLifespan(),
                                                TimeUnit.MILLISECONDS,
                                                tarredSourceQueue,
                                                tarredSourceFactory );

        // Abort policy, but it should not be hit because we throttle submission of tasks to the count of maximum 
        // threads and wait to submit another until after one has get() return.
        this.executor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
    }
}
