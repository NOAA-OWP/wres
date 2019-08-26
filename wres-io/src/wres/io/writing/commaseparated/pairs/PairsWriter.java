package wres.io.writing.commaseparated.pairs;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.pairs.Pair;
import wres.datamodel.sampledata.pairs.Pairs;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCollection;
import wres.datamodel.time.TimeWindow;
import wres.io.writing.WriteException;
import wres.io.writing.commaseparated.CommaSeparatedUtilities;
import wres.util.TimeHelper;

/**
 * <p>Abstract base class for writing a time-series of pairs as comma separated values (CSV). There is one 
 * {@link PairsWriter} for each {@link Path} to be written; writing to that {@link Path} is 
 * managed by this {@link PairsWriter}. The {@link PairsWriter} must be closed after all writing is complete.
 * 
 * <p>The {@link Path} is supplied on construction and no guarantee is made that anything is created at that 
 * {@link Path}. It is the responsibility of the caller to determine whether something was written, if that matters.
 * 
 * @param <S> the decomposed type of pairs to write
 * @param <T> the composed type of pairs to write
 * @author james.brown@hydrosolved.com
 */

public abstract class PairsWriter<S extends Pair<?,?>, T extends Pairs<S> & TimeSeriesCollection<S>>
        implements Consumer<T>, Supplier<Path>, Closeable
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
     * Formatter that maps from a paired value of type <S> to a value of type {@link String}.
     */

    private final Function<S, String> pairFormatter;

    /**
     * Is <code>true</code> if the header needs to be written, <code>false</code> when it has already been written. 
     */

    private final AtomicBoolean isHeaderRequired = new AtomicBoolean( true );   
    
    /**
     * Shared instance of a {@link BufferedWriter} to be closed on completion.
     */

    private BufferedWriter writer = null;
    
    @Override
    public void close() throws IOException
    {
        if( Objects.nonNull( this.writer ) )
        {
            writer.close();
        }        
    }    

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

        // Valid time of pair
        joiner.add( "VALID TIME OF PAIR" );

        // Lead duration
        String leadDurationString = "LEAD DURATION OF PAIR IN " + this.getTimeResolution().toString().toUpperCase();

        // Time scale for lead duration?
        if ( pairs.getMetadata().hasTimeScale() )
        {

            if ( pairs.getMetadata().getTimeScale().isInstantaneous() )
            {
                leadDurationString = leadDurationString
                                     + " "
                                     + pairs.getMetadata().getTimeScale().toString();
            }
            else
            {
                TimeScale s = pairs.getMetadata().getTimeScale();
                leadDurationString = leadDurationString + " ["
                                     + s.getFunction()
                                     + " OVER PAST "
                                     + Long.toString( TimeHelper.durationToLongUnits( s.getPeriod(),
                                                                                      this.getTimeResolution() ) )
                                     + " "
                                     + this.getTimeResolution().toString().toUpperCase()
                                     + "]";
            }
        }

        joiner.add( leadDurationString );

        return joiner;
    }

    /**
     * Supplies the {@link Path} to which values were written.
     * 
     * @return the paths written
     */

    @Override
    public Path get()
    {
        return pathToPairs;
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
            // Write contents if available
            if ( !pairs.getRawData().isEmpty() )
            {
                // Write header if not already written
                // At this point, we have a non-empty pool: #67088
                this.writeHeaderIfRequired( pairs );

                // Feature to write, which is fixed across all pairs
                
                // TODO: need a more consistent representation of a geographic feature throughout the application
                // See #55231-131
                // For now, use the location name OR the coordinates, preferentially
                Location location =  pairs.getMetadata().getIdentifier().getGeospatialID();
                String featureName = location.toString();
                
                if( location.hasLocationName() )
                {
                    featureName = location.getLocationName();
                }
                else if( location.hasCoordinates() )
                {
                    featureName = Float.toString( location.getLongitude() ) + 
                            " " + Float.toString( location.getLatitude() );
                }

                // Time window to write, which is fixed across all pairs
                TimeWindow timeWindow = pairs.getMetadata().getTimeWindow();

                LOGGER.debug( "Writing pairs for {} at time window {} to {}",
                              pairs.getMetadata().getIdentifier(),
                              timeWindow,
                              this.getPath() );

                // Lock for writing
                this.getWriteLock().lock();
                
                LOGGER.trace( "Acquired pair writing lock on {}", this.getPath() );
                
                // Write using shared instance
                BufferedWriter sharedWriter = this.getBufferedWriter();
                
                try
                {

                    // Iterate in time-series order
                    for ( TimeSeries<S> nextSeries : pairs.getTimeSeries() )
                    {
                        Instant basisTime = nextSeries.getReferenceTime();

                        for ( Event<S> nextPair : nextSeries.getEvents() )
                        {

                            // Move to next line
                            sharedWriter.write( System.lineSeparator() );
                            
                            // Compose next line with a string joiner
                            StringJoiner joiner = new StringJoiner( PairsWriter.DELIMITER );

                            // Feature description
                            joiner.add( featureName );

                            // Time window if available
                            if ( Objects.nonNull( timeWindow ) )
                            {
                                joiner.add( timeWindow.getEarliestReferenceTime().toString() );
                                joiner.add( timeWindow.getLatestReferenceTime().toString() );
                                joiner.add( timeWindow.getEarliestValidTime().toString() );
                                joiner.add( timeWindow.getLatestValidTime().toString() );
                                joiner.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getEarliestLeadDuration(),
                                                                                             this.getTimeResolution() ) ) );
                                joiner.add( Long.toString( TimeHelper.durationToLongUnits( timeWindow.getLatestLeadDuration(),
                                                                                             this.getTimeResolution() ) ) );
                            }

                            // ISO8601 datetime string
                            joiner.add( nextPair.getTime().toString() );

                            // Lead duration in standard units
                            joiner.add( Long.toString( TimeHelper.durationToLongUnits( Duration.between( basisTime,
                                                                                                         nextPair.getTime() ),
                                                                                       this.getTimeResolution() ) ) );

                            // Write the values
                            joiner.add( this.getPairFormatter().apply( nextPair.getValue() ) );

                            // Write the composed line
                            sharedWriter.write( joiner.toString() );

                        }

                    }
                    
                    // Flush the buffer
                    sharedWriter.flush();
                    
                    LOGGER.debug( "{} pairs written to {} for {} at time window {}.",
                                  pairs.getRawData().size(),
                                  this.getPath(),
                                  pairs.getMetadata().getIdentifier(),
                                  timeWindow );
                }
                // Clean-up
                finally
                {
                    // Unlock to expose for other writing
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
     * Returns a shared instance of a {@link BufferedWriter}.
     * 
     * @param path the path to write
     * @param appender is true to append, false to truncate
     * @return a writer
     * @throws IOException if the writer cannot be constructed
     */

    private BufferedWriter getBufferedWriter() throws IOException
    {
        if( Objects.isNull( this.writer ) )
        {
            this.getWriteLock().lock();
            
            try
            {
                this.writer = Files.newBufferedWriter( this.getPath(),
                                                       StandardCharsets.UTF_8,
                                                       StandardOpenOption.CREATE,
                                                       StandardOpenOption.TRUNCATE_EXISTING );
            }
            finally
            {
                this.getWriteLock().unlock();
            }
        }

        return this.writer;
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

            // Lock for writing
            this.getWriteLock().lock();
            
            LOGGER.trace( "Acquired pair writing lock on {}", this.getPath() );
            
            // Write using shared instance
            BufferedWriter sharedWriter = this.getBufferedWriter();
            
            try
            {
                // Check again in case header written between first check and lock acquired
                if ( this.isHeaderRequired().get() )
                {
                    sharedWriter.write( header );

                    // Writing succeeded
                    this.isHeaderRequired().set( false );

                    LOGGER.trace( "Header for pairs composed of {} written to {}.", header, this.getPath() );
                }
            }
            // Complete writing
            finally
            {
                // Flush the buffer
                sharedWriter.flush();
                
                // Unlock to expose for other writing
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
