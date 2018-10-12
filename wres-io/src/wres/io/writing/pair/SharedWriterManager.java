package wres.io.writing.pair;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.WRESRunnableException;

/**
 * Stores state that needs to be shared between PairWriter instances.
 * The intent is to only open each pair file *once* and close it *once* per
 * evaluation. If the file exists, it will be deleted and a header written
 * on first check-out.
 */
public class SharedWriterManager implements Closeable,
                                            Consumer<Supplier<Pair<Path,String>>>,
                                            Supplier<Set<Path>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SharedWriterManager.class );

    /**
     * The header to write on new files.
     */
    private static final String OUTPUT_HEADER =
            "FEATURE DESCRIPTION,VALID TIME,LEAD TIME IN " + PairSupplier.DEFAULT_DURATION_UNITS.name().toUpperCase()
                                                + ",TIME WINDOW INDEX,LEFT,RIGHT";

    /**
     * A map of open writers so we don't have to constantly reopen the files.
     */
    private final ConcurrentMap<Path,BufferedWriter> pairWriters;

    /**
     * A map of locks for writers to guard those writers.
     */
    private final ConcurrentMap<Path, ReentrantLock> pairWriterLocks;

    /**
     * ThreadFactory to be used by pairBuildingExecutor (helps name threads)
     */
    private final ThreadFactory pairBuildingThreadFactory =
            runnable -> new Thread( runnable, "Pair Pre-Writing Thread" );

    /**
     * Executor with unbounded queue for pair-building information-gathering
     * string-operations tasks that prepare strings for writing to file.
     */
    private final ExecutorService pairBuildingExecutor = Executors.newSingleThreadExecutor( pairBuildingThreadFactory );

    /**
     * Single thread executor with unbounded queue to reduce write contention
     */
    private final ExecutorService pairWritingExecutor = Executors.newSingleThreadExecutor();

    /**
     * Single instance of a pair line writer to write pairs with.
     */
    private final PairLineWriter pairLineWriter = new PairLineWriter();


    /**
     * Executor with
     */
    private final Set<Path> paths;

    public SharedWriterManager()
    {
        this.pairWriters = new ConcurrentHashMap<>();
        this.pairWriterLocks = new ConcurrentHashMap<>();
        this.paths = new HashSet<>();
    }

    private ConcurrentMap<Path,BufferedWriter> getPairWriters()
    {
        return this.pairWriters;
    }

    private ConcurrentMap<Path,ReentrantLock> getPairWriterLocks()
    {
        return this.pairWriterLocks;
    }


    /**
     * Gets a writer to a path on filesystem.
     *
     * If this is the first time the path was successfully obtained, the file
     * is deleted before being written to, then a header written.
     *
     * Blocks when any other Thread has checked out and not checked in the path.
     *
     * Can be set to package-private if refactoring occurs where private classes
     * that call checkoutExclusiveWriter are moved out of this class.
     *
     * @param path the path to check out for exclusive writing.
     * @return Either an already open writer or a new one.
     * @throws IOException when creation of filewriter fails
     */

    private BufferedWriter checkoutExclusiveWriter( Path path )
            throws IOException
    {
        ReentrantLock writerLock = getWriterLock( path );
        writerLock.lock();

        LOGGER.trace( "Locked path {}: {}", path, writerLock );

        BufferedWriter existingWriter = this.getPairWriters()
                                            .get( path );

        // Best case, the writer exists, return it, leave lock in place
        // because the caller should use returnExclusiveWriter() to unlock.
        if ( Objects.nonNull( existingWriter ) )
        {
            return existingWriter;
        }

        // Since we hold the lock and manage the locks, we can confidently
        // remove old file, create the new writer and write a header

        LOGGER.trace( "deleting {}", path );
        Files.deleteIfExists( path );

        FileWriter fileWriter = new FileWriter( path.toFile(), true );
        BufferedWriter bufferedWriter = new BufferedWriter( fileWriter );

        LOGGER.trace( "writing header to {}", path );
        bufferedWriter.write( OUTPUT_HEADER );
        bufferedWriter.newLine();

        // Publish after we wrote the header.
        BufferedWriter shared = this.getPairWriters()
                                    .putIfAbsent( path, bufferedWriter );

        // Keep track of the fact that this file was created
        this.paths.add( path );

        if ( Objects.isNull( shared ) )
        {
            // We just now created and put the bufferedwriter, return it.
            return bufferedWriter;
        }
        else
        {
            // We did not create the bufferedwriter, meaning there is a bug.
            throw new IllegalStateException( "Did not expect a writer to exist." );
        }
    }

    /**
     * Return a path to allow other Threads to write to that path.
     *
     * Can be set to package-private if refactoring occurs where private classes
     * that call checkoutExclusiveWriter are moved out of this class.
     *
     * Declares that caller is done writing to path, releasing it to others.
     * Should be called in a "finally" block.
     * @param path the path we are done with.
     */

    private void returnExclusiveWriter( Path path )
    {
        Objects.requireNonNull( path );
        ReentrantLock lock = this.getPairWriterLocks().get( path );

        if ( Objects.isNull( lock ) )
        {
            throw new UnsupportedOperationException( "Cannot return a writer that didn't exist." );
        }

        lock.unlock();
        LOGGER.trace( "Unlocked path {}: {}", path, lock );
    }

    /**
     * Get a lock object associated with a file.
     *
     * Intended to remain private regardless of refactoring.
     *
     * @param path the path attempting to be written to
     */

    private ReentrantLock getWriterLock( Path path )
    {
        ReentrantLock existingLock = this.getPairWriterLocks()
                                         .get( path );

        // Best case, the lock exists, return it.
        if ( Objects.nonNull( existingLock ) )
        {
            return existingLock;
        }

        // Trickier case, the lock didn't exist a moment ago but might exist
        // by the time we try to create one.
        ReentrantLock lock = new ReentrantLock();
        ReentrantLock shared = this.getPairWriterLocks()
                                   .putIfAbsent( path, lock );

        if ( Objects.isNull( shared ) )
        {
            // We just now created and put the lock, return it.
            return lock;
        }
        else
        {
            // We did not create the lock, return what was found
            // and discard the one we created.
            return shared;
        }
    }


    @Override
    public void accept( Supplier<Pair<Path,String>> pairCallable )
    {
        
        
        
        
        
        
        // Send the pair building task through a larger executor with a caller
        // runs rejection policy, and send the result of that task to the
        // pair writing executor which is a single thread executor devoted to
        // writing pairs.
        CompletableFuture.supplyAsync( pairCallable, this.pairBuildingExecutor )
                         .thenAcceptAsync( l -> this.pairLineWriter
                                                   .accept( new PairLineBundle( l.getLeft(),
                                                                                l.getRight(),
                                                                                this ) ),
                                           this.pairWritingExecutor );
    }


    /**
     * Get paths assumed to have been written to by checkers-out
     * @return the paths requested by callers during the life of this object
     */

    @Override
    public Set<Path> get()
    {
        return Collections.unmodifiableSet( this.paths );
    }


    /**
     * Flush and close each of the writers, remove them from shared state,
     * remove locks associated with them from shared state after unlocking.
     * Blocks until writers are finished writing to the paths.
     */

    @Override
    public void close()
    {
        this.pairBuildingExecutor.shutdown();

        try
        {
            while (!this.pairBuildingExecutor.awaitTermination( 30,
                                                                TimeUnit.SECONDS ) )
            {
                LOGGER.info( "Waiting for pair writing executor 1 to shut down..." );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while shutting down pair writing executor 1 {}",
                         this.pairBuildingExecutor, ie );
            this.pairBuildingExecutor.shutdownNow();
            this.pairWritingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        this.pairWritingExecutor.shutdown();

        try
        {
            while (!this.pairWritingExecutor.awaitTermination( 30,
                                                                TimeUnit.SECONDS ) )
            {
                LOGGER.info( "Waiting for pair writing executor 2 to shut down..." );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while shutting down pair writing executor 2 {}",
                         this.pairWritingExecutor, ie );
            this.pairWritingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        for ( Map.Entry<Path,BufferedWriter> writer : this.getPairWriters().entrySet() )
        {
            try
            {
                this.checkoutExclusiveWriter( writer.getKey() );
                writer.getValue().flush();
                LOGGER.debug( "Closing writer to path {}: {}", writer.getValue(), writer.getKey() );
                writer.getValue().close();
            }
            catch ( IOException e )
            {
                // Failure to close should not affect primary outputs, still
                // should also attempt to close other writers that may succeed.
                LOGGER.warn( "Failed to flush and close pairs file {}",
                             writer.getKey(), e);
            }
            finally
            {
                this.returnExclusiveWriter( writer.getKey() );
            }
        }

        this.getPairWriters().clear();
        this.getPairWriterLocks().clear();
    }


    /**
     * Composition of a line to write and its dependencies.
     */
    private static class PairLineBundle
    {
        private final Path path;
        private final String toWrite;
        private final SharedWriterManager sharedWriterManager;

        PairLineBundle( Path path,
                        String toWrite,
                        SharedWriterManager sharedWriterManager )
        {
            Objects.requireNonNull( path );
            Objects.requireNonNull( toWrite );
            Objects.requireNonNull( sharedWriterManager );

            this.path = path;
            this.toWrite = toWrite;
            this.sharedWriterManager = sharedWriterManager;
        }

        private Path getPath()
        {
            return this.path;
        }

        private String getToWrite()
        {
            return this.toWrite;
        }

        private SharedWriterManager getSharedWriterManager()
        {
            return this.sharedWriterManager;
        }
    }


    /**
     * Writes a line to a file. Uses a SharedWriterManager to synchronize
     * access to the path, but when using a single instance of PairLineWriter
     * in a single-thread executor, this is possibly redundant. The logic is
     * there to use SharedWriterManager such that multiple of these can be
     * writing simultaneously in a thread safe way.
     */
    private static class PairLineWriter implements Consumer<PairLineBundle>
    {
        @Override
        public void accept( PairLineBundle pairLineBundle )
        {
            try
            {
                BufferedWriter writer =
                        pairLineBundle.getSharedWriterManager()
                                      .checkoutExclusiveWriter( pairLineBundle.getPath() );
                writer.write( pairLineBundle.getToWrite() );
                writer.newLine();
            }
            catch ( IOException ioe )
            {
                throw new WRESRunnableException( "Failed to write pair info '"
                                                 + pairLineBundle.getToWrite() +"' to path "
                                                 + pairLineBundle.getPath(),
                                                 ioe );
            }
            finally
            {
                pairLineBundle.getSharedWriterManager()
                              .returnExclusiveWriter( pairLineBundle.getPath() );
            }
        }
    }
}
