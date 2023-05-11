package wres.io.writing.csv.pairs;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.DataUtilities;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.writing.WriteException;
import wres.io.writing.csv.CommaSeparatedUtilities;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * <p>Abstract base class for writing a time-series of pairs as comma separated values (CSV). There is one 
 * {@link PairsWriter} for each {@link Path} to be written; writing to that {@link Path} is managed by this 
 * {@link PairsWriter}. The {@link PairsWriter} must be closed after all writing is complete.
 * 
 * <p>The {@link Path} is supplied on construction and no guarantee is made that anything is created at that 
 * {@link Path}. If nothing is created, then {@link #get()} will return the {@link Collections#emptySet()}.
 * 
 * <p>TODO: Add some additional qualification to the pairs, such as the left and right feature names (separately).
 * However, pairs are modeled as time-series of pairs, not pairs of time-series, so the time-series metadata only
 * accommodates a {@link Feature}, not a {@link FeatureTuple}. Currently, the time-series metadata contains the
 * {@link Feature} associated with the evaluation subject or right-hand data only. There are various possible fixes.
 * Ultimately, the thing abstracted by a time-series event has some metadata, so the most flexible approach would be to
 * improve the modeling of that thing, i.e., of the pair, by adding a {@link FeatureTuple} to it. An alternative
 * approach would be to model a {@code TimeSeriesOfPairs<L,R> extends TimeSeries<Pair<L,R>>} that composes the
 * time-series of pairs, plus both sides of time-series metadata. However, this class would then need to consume a
 * {@code PoolOfPairs<L, R>} that composed the {@code TimeSeriesOfPairs<L,R>}. Another alternative would be to replace
 * the {@link Feature} within the {@link TimeSeriesMetadata} with a {@link FeatureTuple}.
 * 
 * @param <L> the type of left data in the pairing
 * @param <R> the type of right data in the pairing
 * @author James Brown
 */

public abstract class PairsWriter<L, R>
        implements Consumer<Pool<TimeSeries<Pair<L, R>>>>, Supplier<Set<Path>>, Closeable
{

    /**
     * Double quotation mark.
     */

    private static final String QUOTE = "\"";

    /**
     * A default name for the pairs.
     */

    public static final String DEFAULT_PAIRS_NAME = "pairs.csv";

    /**
     * A default name for the pairs.
     */

    public static final String DEFAULT_PAIRS_ZIP_NAME = "pairs.zip";

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
     * Boolean determining if output is gzip
     */
    private final boolean gzip;

    /**
     * The time resolution.
     */

    private final ChronoUnit timeResolution;

    /**
     * Formatter that maps from a paired value to a {@link String}.
     */

    private final Function<Pair<L, R>, String> pairFormatter;

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
        if ( Objects.nonNull( this.writer ) )
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

    StringJoiner getHeaderFromPairs( Pool<TimeSeries<Pair<L, R>>> pairs )
    {
        Objects.requireNonNull( pairs, "Cannot obtain header from null pairs." );

        StringJoiner joiner = new StringJoiner( "," );

        joiner.add( "FEATURE"
                    + CommaSeparatedUtilities.HEADER_DELIMITER
                    + "DESCRIPTION" )
              .add( "FEATURE"
                    + CommaSeparatedUtilities.HEADER_DELIMITER
                    + "GROUP"
                    + CommaSeparatedUtilities.HEADER_DELIMITER
                    + "NAME" );

        // Time window?
        if ( pairs.getMetadata().hasTimeWindow() )
        {
            joiner.merge( CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( pairs.getMetadata(),
                                                                                         this.getTimeResolution() ) );
        }

        // Valid time of pair
        joiner.add( "VALID"
                    + CommaSeparatedUtilities.HEADER_DELIMITER
                    + "TIME"
                    + CommaSeparatedUtilities.HEADER_DELIMITER
                    + "OF"
                    + CommaSeparatedUtilities.HEADER_DELIMITER
                    + "PAIR" );

        // Lead duration
        String leadDurationString = "LEAD" + CommaSeparatedUtilities.HEADER_DELIMITER
                                    + "DURATION"
                                    + CommaSeparatedUtilities.HEADER_DELIMITER
                                    + "OF"
                                    + CommaSeparatedUtilities.HEADER_DELIMITER
                                    + "PAIR"
                                    + CommaSeparatedUtilities.HEADER_DELIMITER
                                    + "IN"
                                    + CommaSeparatedUtilities.HEADER_DELIMITER
                                    + this.getTimeResolution()
                                          .toString()
                                          .toUpperCase();

        // Time scale for lead duration?
        if ( pairs.getMetadata().hasTimeScale() )
        {
            TimeScaleOuter timeScaleOuter = pairs.getMetadata().getTimeScale();
            leadDurationString =
                    leadDurationString + CommaSeparatedUtilities.HEADER_DELIMITER
                                 + CommaSeparatedUtilities.getTimeScaleForHeader( timeScaleOuter,
                                                                                  this.getTimeResolution() );
        }

        joiner.add( leadDurationString );

        return joiner;
    }

    /**
     * Supplies the {@link Path} to which values were written or the empty set if nothing was written.
     * 
     * @return the paths written
     */

    @Override
    public Set<Path> get()
    {
        Set<Path> returnMe = new HashSet<>();

        // Path points to a file that exists?
        if ( Files.exists( this.pathToPairs ) )
        {
            returnMe.add( this.pathToPairs );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Write the pairs.
     * 
     * @param pairs the pairs to write
     * @throws NullPointerException if the input is null or required metadata is null
     * @throws WriteException if the writing fails
     */
    @Override
    public void accept( Pool<TimeSeries<Pair<L, R>>> pairs )
    {
        Objects.requireNonNull( pairs, "Cannot write null pairs." );

        if ( pairs.getMetadata()
                  .getPool()
                  .getGeometryGroup()
                  .getGeometryTuplesCount() == 0 )
        {
            throw new WriteException( "Cannot write pairs with an empty list of geometries. The pair metadata was: "
                                      + pairs.getMetadata()
                                      + "." );
        }

        try
        {
            // Write contents if available
            if ( PoolSlicer.getPairCount( pairs ) > 0 )
            {
                // Write header if not already written
                // At this point, we have a non-empty pool: #67088
                this.writeHeaderIfRequired( pairs );

                // Feature group name
                GeometryGroup geoGroup = pairs.getMetadata().getPool().getGeometryGroup();
                String featureGroupName = this.getFeatureNameFrom( geoGroup.getRegionName() );

                // Time window to write, which is fixed across all pairs
                TimeWindowOuter timeWindow = pairs.getMetadata().getTimeWindow();

                LOGGER.debug( "Writing pairs for feature group {} at time window {} to {}",
                              pairs.getMetadata().getFeatureGroup(),
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
                    for ( TimeSeries<Pair<L, R>> nextSeries : pairs.get() )
                    {
                        String featureName = this.getFeatureNameFrom( nextSeries.getMetadata()
                                                                                .getFeature()
                                                                                .getName() );

                        // Iterate the events
                        for ( Event<Pair<L, R>> nextPair : nextSeries.getEvents() )
                        {

                            // Move to next line
                            sharedWriter.write( System.lineSeparator() );

                            // Compose next line with a string joiner
                            StringJoiner joiner = new StringJoiner( PairsWriter.DELIMITER );

                            // Feature description
                            joiner.add( featureName );

                            // Feature group description
                            joiner.add( featureGroupName );

                            // Time window if available
                            if ( Objects.nonNull( timeWindow ) )
                            {
                                joiner.add( timeWindow.getEarliestReferenceTime().toString() );
                                joiner.add( timeWindow.getLatestReferenceTime().toString() );
                                joiner.add( timeWindow.getEarliestValidTime().toString() );
                                joiner.add( timeWindow.getLatestValidTime().toString() );
                                joiner.add( DataUtilities.durationToNumericUnits( timeWindow.getEarliestLeadDuration(),
                                                                                this.getTimeResolution() )
                                                       .toString() );
                                joiner.add( DataUtilities.durationToNumericUnits( timeWindow.getLatestLeadDuration(),
                                                                                this.getTimeResolution() )
                                                       .toString() );
                            }

                            // ISO8601 datetime string
                            joiner.add( nextPair.getTime().toString() );

                            // Choose one. TODO: include reference times, rather than lead durations, and
                            // then print all reference times
                            Instant referenceTime = this.getFirstReferenceTime( nextSeries );
                            // Lead duration in standard units
                            Duration leadDuration = this.getLeadDuration( referenceTime, nextPair.getTime() );
                            joiner.add( DataUtilities.durationToNumericUnits( leadDuration,
                                                                            this.getTimeResolution() )
                                                   .toString() );

                            // Write the values
                            joiner.add( this.getPairFormatter().apply( nextPair.getValue() ) );

                            // Write the composed line
                            sharedWriter.write( joiner.toString() );

                        }

                    }

                    // Flush the buffer
                    sharedWriter.flush();

                    if ( LOGGER.isDebugEnabled() )
                    {
                        LOGGER.debug( "{} time-series of pairs containing {} pairs written to {} for feature group {} "
                                      + "at time window {}.",
                                      pairs.get().size(),
                                      PoolSlicer.getPairCount( pairs ),
                                      this.getPath(),
                                      pairs.getMetadata().getFeatureGroup(),
                                      timeWindow );
                    }
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
     * Returns the flag for if a file should be gzip
     *
     * @return the gzip flag
     */
    boolean getGzip()
    {
        return this.gzip;
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
     * Returns the reference time from the next series, if any.
     * 
     * @return the reference time, if any
     */

    private Instant getFirstReferenceTime( TimeSeries<?> timeSeries )
    {
        Objects.requireNonNull( timeSeries );

        Map<ReferenceTimeType, Instant> referenceTimes = timeSeries.getReferenceTimes();

        Instant referenceTime = null;
        if ( !referenceTimes.isEmpty() )
        {
            referenceTime = referenceTimes.values().iterator().next();
        }

        return referenceTime;
    }

    /**
     * Returns the lead duration associated with the input.
     * 
     * @param referenceTime the reference time
     * @param validTime the valid time
     * @return the lead duration
     */

    private Duration getLeadDuration( Instant referenceTime, Instant validTime )
    {
        if ( Objects.nonNull( referenceTime ) )
        {
            return Duration.between( referenceTime, validTime );
        }

        return Duration.ZERO;
    }

    /**
     * Returns a shared instance of a {@link BufferedWriter}. Close with the overall writer by calling {@link #close()}.
     * @return a writer
     * @throws IOException if the writer cannot be constructed
     */

    private BufferedWriter getBufferedWriter() throws IOException
    {
        if ( Objects.isNull( this.writer ) )
        {
            this.getWriteLock()
                .lock();

            try  // NOSONAR
            {
                if ( this.getGzip() )
                {
                    GZIPOutputStream zip = new GZIPOutputStream( Files.newOutputStream( this.getPath(),
                                                                                        StandardOpenOption.CREATE,
                                                                                        StandardOpenOption.APPEND ) );
                    this.writer = new BufferedWriter( new OutputStreamWriter( zip, StandardCharsets.UTF_8 ) );
                }
                else
                {
                    this.writer = Files.newBufferedWriter( this.getPath(),
                                                           StandardCharsets.UTF_8,
                                                           StandardOpenOption.CREATE,
                                                           StandardOpenOption.TRUNCATE_EXISTING );
                }
            }
            finally
            {
                this.getWriteLock()
                    .unlock();
            }
        }

        return this.writer;
    }

    /**
     * Helper that writes the specified header information if it has not been written already.
     * 
     * @param pairs the pairs
     * @throws IOException if the header cannot be written
     * @throws NullPointerException if the header is null
     */

    private void writeHeaderIfRequired( final Pool<TimeSeries<Pair<L, R>>> pairs ) throws IOException
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

    private Function<Pair<L, R>, String> getPairFormatter()
    {
        return this.pairFormatter;
    }

    /**
     * Returns a sanitized feature name from the input.
     * 
     * @param featureName the unsanitized name
     * @return the sanitized name
     */

    private String getFeatureNameFrom( String featureName )
    {
        if ( Objects.nonNull( featureName ) && featureName.contains( "," ) )
        {
            return QUOTE + featureName + QUOTE;
        }

        return featureName;
    }

    /**
     * Hidden constructor.
     * 
     * @param pathToPairs the required path to write
     * @param timeResolution the required time resolution at which to write datetime and duration information
     * @param formatter the required formatter for writing pairs
     * @param gzip boolean to determine if output should be gzip
     * @throws NullPointerException if any of the expected inputs is null
     */

    PairsWriter( Path pathToPairs,
                 ChronoUnit timeResolution,
                 Function<Pair<L, R>, String> formatter,
                 boolean gzip )
    {
        Objects.requireNonNull( pathToPairs, "Specify a non-null path to write." );

        Objects.requireNonNull( timeResolution, "Specify a non-null time resolution for writing pairs." );

        Objects.requireNonNull( formatter, "Specify a non-null time resolution for writing pairs." );

        this.pathToPairs = pathToPairs;
        this.timeResolution = timeResolution;
        this.pairFormatter = formatter;
        this.gzip = gzip;

        this.writeLock = new ReentrantLock();

        LOGGER.trace( "Will write pairs to {}.", this.getPath() );
    }
}
