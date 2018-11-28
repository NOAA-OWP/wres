package wres.io.writing.commaseparated.pairs;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metadata.TimeScale;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.pairs.Pairs;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.io.writing.WriteException;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;
import wres.util.TimeHelper;

/**
 * Abstract base class for writing a time-series of pairs as comma separated values (CSV). There is one 
 * {@link PairsWriter} for each {@link Path} to be written; writing to that {@link Path} is 
 * managed by this {@link PairsWriter}.
 * 
 * @param <S> the decomposed type of pairs to write
 * @param <T> the composed type of pairs to write
 * @author james.brown@hydrosolved.com
 */

public abstract class PairsWriter<S extends Object, T extends Pairs<S> & TimeSeries<S>>
        implements Consumer<T>, Supplier<Set<Path>>
{

    /**
     * A default name for the pairs.
     */

    public static final String DEFAULT_PAIRS_NAME = "pairs_new.csv";

    /**
     * Delimiter.
     */

    public static final String DELIMITER = ",";

    /**
     * A default name for the baseline pairs.
     */

    public static final String DEFAULT_BASELINE_PAIRS_NAME = "baseline_pairs_new.csv";

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
     * Formatter that maps from a paired value of type <S> to a value of type {@link String}.
     */

    private final Function<S, String> pairFormatter;

    /**
     * Is <code>true</code> if the header needs to be written, <code>false</code> when it has already been written. 
     */

    private final AtomicBoolean isHeaderRequired = new AtomicBoolean( true );

    /**
     * Returns a basic header for the pairs from the input. Override this method to add information for specific types
     * of pairs.
     * 
     * @param pairs the pairs
     * @throws NullPointerException if the pairs are null
     */

    StringJoiner getHeaderFromPairs( T pairs )
    {
        Objects.requireNonNull( pairs, "Cannot obtain header from null pairs." );

        StringJoiner joiner = new StringJoiner( "," );

        joiner.add( "FEATURE DESCRIPTION" );

        // Time window?
        if ( pairs.getMetadata().hasTimeWindow() )
        {
            joiner.merge( CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( pairs.getMetadata(),
                                                                                         this.getTimeResolution() ) );
        }
        joiner.add( "VALID TIME OF PAIR" );

        if ( pairs.getMetadata().hasTimeScale() )
        {
            TimeScale timeScale = pairs.getMetadata().getTimeScale();

            joiner.add( "LEAD DURATION OF PAIR IN " + this.getTimeResolution().toString().toUpperCase()
                        + " ["
                        + timeScale.getFunction()
                        + " OVER PAST "
                        + timeScale.getPeriod().get( this.getTimeResolution() )
                        + " "
                        + this.getTimeResolution().toString().toUpperCase()
                        + "]" );
        }
        else
        {
            joiner.add( "LEAD DURATION OF PAIR IN " + this.getTimeResolution().toString().toUpperCase() );
        }

        return joiner;
    }

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
     * Write the pairs.
     * 
     * @param pairs the pairs to write
     * @throws NullPointerException if the input is null or required metadata is null
     * @throws WriteException if the writing fails
     */

    @Override
    public void accept( T pairs )
    {
        Objects.requireNonNull( pairs, "Cannot write null pairs." );

        Objects.requireNonNull( pairs.getMetadata().getIdentifier(),
                                "Cannot write pairs with a null dataset identifier." );

        Objects.requireNonNull( pairs.getMetadata().getIdentifier().getGeospatialID(),
                                "Cannot write pairs with a null geospatial identifier." );

        try
        {
            // Write header
            this.writeHeaderIfRequired( pairs );

            // Write contents if available
            if ( !pairs.getRawData().isEmpty() )
            {
                LOGGER.debug( "Writing pairs for {} to {}", pairs.getMetadata().getIdentifier(), this.getPath() );

                // Feature to write, which is fixed across all pairs
                String featureName = pairs.getMetadata().getIdentifier().getGeospatialID().getLocationName();

                // Time window to write, which is fixed across all pairs
                TimeWindow timeWindow = pairs.getMetadata().getTimeWindow();

                // Prepare
                this.getWriteLock().lock();
                LOGGER.trace( "Acquired pair writing lock on {}", this.getPath() );

                // Write by appending
                try ( BufferedWriter writer = this.getBufferedWriter( true ) )
                {
                    // Iterate in time-series order
                    for ( TimeSeries<S> nextSeries : pairs.basisTimeIterator() )
                    {

                        Instant basisTime = nextSeries.getEarliestBasisTime();

                        for ( Event<S> nextPair : nextSeries.timeIterator() )
                        {

                            // Move to next line
                            writer.write( System.lineSeparator() );
                            
                            StringJoiner joiner = new StringJoiner( PairsWriter.DELIMITER );

                            // Feature description
                            joiner.add( featureName );

                            // Time window if available
                            if ( Objects.nonNull( timeWindow ) )
                            {
                                joiner.add( timeWindow.getEarliestReferenceTime().toString() );
                                joiner.add( timeWindow.getLatestReferenceTime().toString() );
                                joiner.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getEarliestLeadDuration(),
                                                                                             this.getTimeResolution() ) ) );
                                joiner.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                                             this.getTimeResolution() ) ) );
                            }

                            // ISO8601 datetime string
                            joiner.add( nextPair.getTime().toString() );

                            // Lead duration in standard units
                            joiner.add( Long.toString( Duration.between( basisTime, nextPair.getTime() )
                                                               .get( this.getTimeResolution() ) ) );

                            // Write the values
                            joiner.add( this.getPairFormatter().apply( nextPair.getValue() ) );

                            // Write next line
                            writer.write( joiner.toString() );

                        }
                    }

                    LOGGER.trace( "{} pairs written to {}.", pairs.getRawData().size(), this.getPath() );
                }
                // Clean-up
                finally
                {
                    this.getWriteLock().unlock();
                    LOGGER.trace( "Released pair writing lock on {}", this.getPath() );
                }
            }
            else
            {
                LOGGER.debug( "No pairs written to {} as the pairs were empty.", this.getPath() );
            }
        }
        catch ( IOException e )
        {
            throw new WriteException( "Unable to write pairs.", e );
        }
    }

    /**
     * Returns the time resolution at which pairs should be written.
     * 
     * @return the time resolution
     */

    ChronoUnit getTimeResolution()
    {
        return this.timeResolution;
    }

    /**
     * Returns the path to the pairs.
     * 
     * @return the path to the pairs.
     */

    private Path getPath()
    {
        return this.pathToPairs;
    }

    /**
     * Returns the lock to use when writing pairs.
     * 
     * @return the write lock
     */

    private ReentrantLock getWriteLock()
    {
        return this.writeLock;
    }

    /**
     * Returns a writer that either appends or truncates.
     * 
     * @return a writer
     * @param appender is true to append, false to truncate
     * @throws IOException if the writer cannot be constructed
     */

    private BufferedWriter getBufferedWriter( boolean appender ) throws IOException
    {
        StandardOpenOption appendOrTruncate = StandardOpenOption.TRUNCATE_EXISTING;

        if ( appender )
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

    private void writeHeaderIfRequired( final T pairs ) throws IOException
    {
        Objects.requireNonNull( pairs, "Specify a non-null header for writing pairs." );

        // Header required?
        if ( this.isHeaderRequired().get() )
        {
            // Acquire the header
            final String header = this.getHeaderFromPairs( pairs ).toString();

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
     * Returns the formatter for writing paired values.
     * 
     * @return the formatter
     */

    private Function<S, String> getPairFormatter()
    {
        return this.pairFormatter;
    }

    /**
     * Hidden constructor.
     * 
     * @param <S> the type of pairs to write
     * @param pathToPairs the required path to write
     * @param timeResolution the required time resolution at which to write datetime and duration information
     * @param formatter the required formatter for writing pairs
     * @throws NullPointerException if any of the expected inputs is null
     */

    PairsWriter( Path pathToPairs, ChronoUnit timeResolution, Function<S, String> formatter )
    {
        Objects.requireNonNull( pathToPairs, "Specify a non-null path to write." );

        Objects.requireNonNull( timeResolution, "Specify a non-null time resolution for writing pairs." );

        Objects.requireNonNull( formatter, "Specify a non-null time resolution for writing pairs." );

        this.pathToPairs = pathToPairs;
        this.timeResolution = timeResolution;
        this.pairFormatter = formatter;

        this.writeLock = new ReentrantLock();

        LOGGER.trace( "Will write pairs to {}.", this.getPath() );
    }
}
