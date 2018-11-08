package wres.io.writing.commaseparated.pairs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.pairs.Pairs;

/**
 * Abstract base class for writing a collection of pairs as comma separated values (CSV). There is one 
 * {@link PairsWriter} for each {@link Path} to be written; writing to that {@link Path} is 
 * managed by this {@link PairsWriter}.
 * 
 * @param <T> the type of pairs to write
 * @author james.brown@hydrosolved.com
 */

abstract class PairsWriter<T extends Pairs<?>> implements Consumer<T>, Supplier<Set<Path>>
{

    /**
     * A default name for the pairs.
     */

    public static final String DEFAULT_PAIRS_NAME = "pairs.csv";
    
    /**
     * Delimiter.
     */

    public static final String DELIMITER = ",";
    
    /**
     * A default name for the baseline pairs.
     */

    public static final String DEFAULT_BASELINE_PAIRS_NAME = "baseline_pairs.csv";

    /**
     * Logger.
     */

    static final Logger LOGGER = LoggerFactory.getLogger( PairsWriter.class );

    /**
     * Lock for writing pairs to the {@link #pathToPairs} for which this writer is built.
     */

    private final ReentrantLock writeLock;

    /**
     * Path to write.
     */

    private final Path pathToPairs;
    
    /**
     * The time resolution.
     */

    private final ChronoUnit timeResolution;
    
    /**
     * Optional decimal formatter.
     */
    
    private final DecimalFormat formatter;

    /**
     * Is <code>true</code> if the header needs to be written, <code>false</code> when it has already been written. 
     */

    private final AtomicBoolean isHeaderRequired = new AtomicBoolean( true );

    /**
     * Returns a header for the pairs from the input.
     * 
     * @param pairs the pairs
     * @throws NullPointerException if the pairs are null
     */

    abstract String getHeaderFromPairs( T pairs );

    /**
     * Supplies the set of {@link Path} to which values were written.
     * 
     * @return the paths written
     */

    @Override
    public Set<Path> get()
    {
        return Collections.singleton( pathToPairs );
    }

    /**
     * Returns the time resolution at which pairs should be written.
     * 
     * @return the time resolution
     */
    
    public ChronoUnit getTimeResolution()
    {
        return this.timeResolution;
    }
    
    /**
     * Returns the path to the pairs.
     * 
     * @return the path to the pairs.
     */

    Path getPath()
    {
        return this.pathToPairs;
    }

    /**
     * Returns the lock to use when writing pairs.
     * 
     * @return the write lock
     */

    ReentrantLock getWriteLock()
    {
        return this.writeLock;
    }

    /**
     * Returns the decimal formatter or null if no formatter is defined.
     * 
     * @return the formatter or null
     */
    
    DecimalFormat getDecimalFormatter()
    {
        return this.formatter;
    }
    
    /**
     * Returns a writer that either appends or truncates.
     * 
     * @return a writer
     * @param appender is true to append, false to truncate
     * @throws IOException if the writer cannot be constructed
     */

    BufferedWriter getBufferedWriter( boolean appender ) throws IOException
    {
        StandardOpenOption appendOrTruncate = StandardOpenOption.TRUNCATE_EXISTING;
        
        if( appender )
        {
            appendOrTruncate = StandardOpenOption.APPEND;
        }
        
        return Files.newBufferedWriter( this.getPath(),
                                        StandardCharsets.UTF_8,
                                        StandardOpenOption.CREATE,
                                        appendOrTruncate );
    }

    /**
     * Helper that writes the specified header information if it has not been written already.
     * 
     * @param <T> the type of pairs for which a header is required
     * @param pairs the pairs
     * @throws IOException if the header cannot be written
     * @throws NullPointerException if the header is null
     */

    void writeHeaderIfRequired( final T pairs ) throws IOException
    {
        Objects.requireNonNull( pairs, "Specify a non-null header for writing pairs." );

        // Header required?
        if ( this.isHeaderRequired().get() )
        {
            // Acquire the header
            final String header = this.getHeaderFromPairs( pairs );

            // Prepare to write
            this.getWriteLock().lock();
            LOGGER.trace( "Acquired pair writing lock on {}", this.getPath() );
            
            // Write by truncation
            try ( BufferedWriter writer = this.getBufferedWriter( false ) )
            {
                writer.write( header );

                // Writing succeeded
                this.isHeaderRequired().set( false );

                LOGGER.trace( "Header for pairs composed of {} written to {}.", header, this.getPath() );
            }
            // Complete writing
            finally
            {
                this.getWriteLock().unlock();
                LOGGER.trace( "Released pair writing lock on {}", this.getPath() );
            }
        }
    }

    /**
     * Returns <code>true</code> is the header needs to be written, otherwise <code>false</code>.
     * 
     * @return true to write the header, otherwise false
     */

    private AtomicBoolean isHeaderRequired()
    {
        return this.isHeaderRequired;
    }

    /**
     * Hidden constructor.
     * 
     * @param <T> the type of pairs to write
     * @param pathToPairs the path to write
     * @param timeResolution the time resolution at which to write datetime and duration information
     * @param formatter the optional formatter for writing decimal values
     * @throws NullPointerException if any of the expected inputs is null
     */

    PairsWriter( Path pathToPairs, ChronoUnit timeResolution, DecimalFormat formatter )
    {
        Objects.requireNonNull( pathToPairs, "Specify a non-null path to write." );

        Objects.requireNonNull( timeResolution, "Specify a non-null time resolution for writing pairs." );

        this.pathToPairs = pathToPairs;
        this.timeResolution = timeResolution;
        this.formatter = formatter;
        this.writeLock = new ReentrantLock();

        LOGGER.trace( "Will write pairs to {}.", this.getPath() );
    }
}
