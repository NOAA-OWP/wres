package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.Timestamp;

import net.jcip.annotations.Immutable;

import wres.datamodel.messages.MessageUtilities;
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeWindow;


/**
 * <p>Wraps a canonical {@link TimeWindow} and adds behavior. Describes the partition of three time lines in which a
 * sample is collated for the purposes of a statistical calculation. The first timeline is a reference timeline in UTC,
 * which may be used to represent the origin of a forecast (e.g. the issued datetime). The second timeline represents
 * the valid datetime of a measurement, also in UTC. The third timeline describes the duration of a measurement, such
 * as a forecast lead duration. Each timeline is bounded with an earliest and latest bookend. A {@link TimeWindowOuter}
 * represents the intersection of these three timelines, i.e., each of its elements are members of each of the three
 * timelines.
 * 
 * <p>In summary, a {@link TimeWindowOuter} comprises the following required elements:
 * 
 * <ol>
 * <li>The earliest reference time</li>
 * <li>The latest reference time</li>
 * <li>The earliest valid time
 * <li>The latest valid time
 * <li>The earliest duration</li>
 * <li>The latest duration</li>
 * </ol>
 * 
 * <p><b>Implementation Requirements:</b>
 * 
 * <p>TODO: If a future JDK implements something equivalent to an Interval in joda.time, consider replacing the 
 * earliest time and latest time with that abstraction.
 *
 * @author James Brown
 */

@Immutable
public class TimeWindowOuter implements Comparable<TimeWindowOuter>
{
    /**
     * <p>Minimum {@link Duration}. 
     * 
     * <p>TODO: if the JDK adds something similar to {@link Instant#MIN} for a {@link Duration}, 
     * remove this.
     */

    public static final Duration DURATION_MIN = Duration.ofSeconds( Long.MIN_VALUE );

    /**
     * <p>Maximum {@link Duration}. 
     * 
     * <p>TODO: if the JDK adds something similar to {@link Instant#MAX} for a {@link Duration}, 
     * remove this.
     */

    public static final Duration DURATION_MAX = Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 );

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeWindowOuter.class );

    /** Cache of time windows, one per class loader. Arbitrarily constrained, more than a handful. */
    private static final Cache<TimeWindow, TimeWindowOuter> TIME_WINDOW_CACHE = Caffeine.newBuilder()
                                                                                        .maximumSize( 500 )
                                                                                        .build();

    /** The canonical description. */
    private final TimeWindow timeWindow;

    /**
     * Constructs a {@link TimeWindowOuter} with a canonical {@link TimeWindow}.
     * 
     * @see MessageFactory#getTimeWindow() and related methods
     * @param timeWindow a time window
     * @return a time window
     */

    public static TimeWindowOuter of( TimeWindow timeWindow )
    {
        // Check the cache
        TimeWindowOuter cached = TIME_WINDOW_CACHE.getIfPresent( timeWindow );
        if ( Objects.nonNull( cached ) )
        {
            return cached;
        }

        TimeWindowOuter newInstance = new TimeWindowOuter( timeWindow );
        TIME_WINDOW_CACHE.put( timeWindow, newInstance );
        return newInstance;
    }

    /**
     * Returns a {@link TimeWindowOuter} that represents the union of the inputs, specifically where the
     * {@link #getEarliestReferenceTime()} and {@link #getLatestReferenceTime()} are the earliest and latest instances, 
     * respectively, and likewise for the {@link #getEarliestValidTime()} and {@link #getLatestValidTime()}, and the 
     * {@link #getEarliestLeadDuration()} and {@link #getLatestLeadDuration()}.
     * 
     * @param input the input windows
     * @return the union of the inputs with respect to dates and lead times
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter unionOf( Set<TimeWindowOuter> input )
    {
        Objects.requireNonNull( input, "Cannot determine the union of time windows for a null input." );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot determine the union of time windows for empty input." );
        }

        if ( new HashSet<>( input ).contains( null ) )
        {
            throw new IllegalArgumentException( "Cannot determine the union of time windows for input that contains "
                                                + "one or more null time windows." );
        }

        // Check and set time parameters
        TimeWindowOuter first = input.iterator().next();
        Instant earliestR = first.getEarliestReferenceTime();
        Instant latestR = first.getLatestReferenceTime();
        Instant earliestV = first.getEarliestValidTime();
        Instant latestV = first.getLatestValidTime();
        Duration earliestL = first.getEarliestLeadDuration();
        Duration latestL = first.getLatestLeadDuration();

        for ( TimeWindowOuter next : input )
        {
            if ( earliestR.isAfter( next.getEarliestReferenceTime() ) )
            {
                earliestR = next.getEarliestReferenceTime();
            }
            if ( latestR.isBefore( next.getLatestReferenceTime() ) )
            {
                latestR = next.getLatestReferenceTime();
            }
            if ( earliestL.compareTo( next.getEarliestLeadDuration() ) > 0 )
            {
                earliestL = next.getEarliestLeadDuration();
            }
            if ( latestL.compareTo( next.getLatestLeadDuration() ) < 0 )
            {
                latestL = next.getLatestLeadDuration();
            }
            if ( earliestV.isAfter( next.getEarliestValidTime() ) )
            {
                earliestV = next.getEarliestValidTime();
            }
            if ( latestV.isBefore( next.getLatestValidTime() ) )
            {
                latestV = next.getLatestValidTime();
            }
        }

        TimeWindow unionWindow = wres.statistics.MessageFactory.getTimeWindow( earliestR,
                                                                               latestR,
                                                                               earliestV,
                                                                               latestV,
                                                                               earliestL,
                                                                               latestL );
        return TimeWindowOuter.of( unionWindow );
    }

    @Override
    public int compareTo( TimeWindowOuter o )
    {
        return MessageUtilities.compare( this.getTimeWindow(), o.getTimeWindow() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof TimeWindowOuter in ) )
        {
            return false;
        }

        return Objects.equals( this.getTimeWindow(), in.getTimeWindow() );
    }

    @Override
    public int hashCode()
    {
        return this.getTimeWindow()
                   .hashCode();
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "earliestReferenceTime",
                                                                                     this.getEarliestReferenceTime() )
                                                                            .append( "latestReferenceTime",
                                                                                     this.getLatestReferenceTime() )
                                                                            .append( "earliestValidTime",
                                                                                     this.getEarliestValidTime() )
                                                                            .append( "latestValidTime",
                                                                                     this.getLatestValidTime() )
                                                                            .append( "earliestLeadDuration",
                                                                                     this.getEarliestLeadDuration() )
                                                                            .append( "latestLeadDuration",
                                                                                     this.getLatestLeadDuration() )
                                                                            .toString();
    }

    /**
     * Returns the earliest reference time {@link Instant} associated with the time window
     * 
     * @return the earliest reference time instant
     */

    public Instant getEarliestReferenceTime()
    {
        Timestamp timeStamp = this.timeWindow.getEarliestReferenceTime();

        return Instant.ofEpochSecond( timeStamp.getSeconds(), timeStamp.getNanos() );
    }

    /**
     * Returns the latest reference time {@link Instant} associated with the time window
     * 
     * @return the latest reference time instant
     */

    public Instant getLatestReferenceTime()
    {
        Timestamp timeStamp = this.timeWindow.getLatestReferenceTime();

        return Instant.ofEpochSecond( timeStamp.getSeconds(), timeStamp.getNanos() );
    }

    /**
     * Returns the earliest valid time {@link Instant} associated with the time window
     * 
     * @return the earliest valid time instant
     */

    public Instant getEarliestValidTime()
    {
        Timestamp timeStamp = this.timeWindow.getEarliestValidTime();

        return Instant.ofEpochSecond( timeStamp.getSeconds(), timeStamp.getNanos() );
    }

    /**
     * Returns the latest valid time {@link Instant} associated with the time window
     * 
     * @return the latest valid time instant
     */

    public Instant getLatestValidTime()
    {
        Timestamp timeStamp = this.timeWindow.getLatestValidTime();

        return Instant.ofEpochSecond( timeStamp.getSeconds(), timeStamp.getNanos() );
    }

    /**
     * Returns <code>true</code> if {@link #getEarliestReferenceTime()} returns {@link Instant#MIN} or 
     * {@link #getLatestReferenceTime()} returns {@link Instant#MAX}, otherwise <code>false</code>.
     * 
     * @return true if the timeline is unbounded, false otherwise
     */

    public boolean hasUnboundedReferenceTimes()
    {
        return Instant.MIN.equals( this.getEarliestReferenceTime() )
               || Instant.MAX.equals( this.getLatestReferenceTime() );
    }

    /**
     * Returns <code>true</code> if {@link #getEarliestValidTime()} returns {@link Instant#MIN} or 
     * {@link #getLatestValidTime()} returns {@link Instant#MAX}, otherwise <code>false</code>.
     * 
     * @return true if the timeline is unbounded, false otherwise
     */

    public boolean hasUnboundedValidTimes()
    {
        return Instant.MIN.equals( this.getEarliestValidTime() ) || Instant.MAX.equals( this.getLatestValidTime() );
    }

    /**
     * Returns <code>true</code> if {@link #getEarliestLeadDuration()} returns {@link TimeWindowOuter#DURATION_MIN} and 
     * {@link #getLatestLeadDuration()} returns {@link TimeWindowOuter#DURATION_MIN}, otherwise <code>false</code>.
     * 
     * @return true if the timeline is unbounded, false otherwise
     */

    public boolean bothLeadDurationsAreUnbounded()
    {
        return TimeWindowOuter.DURATION_MIN.equals( this.getEarliestLeadDuration() )
               && TimeWindowOuter.DURATION_MAX.equals( this.getLatestLeadDuration() );
    }

    /**
     * Returns the earliest forecast lead time.
     * 
     * @return the earliest lead time
     */

    public Duration getEarliestLeadDuration()
    {
        com.google.protobuf.Duration duration = this.timeWindow.getEarliestLeadDuration();

        return Duration.ofSeconds( duration.getSeconds(), duration.getNanos() );
    }

    /**
     * Returns the latest forecast lead time.
     * 
     * @return the latest lead time
     */

    public Duration getLatestLeadDuration()
    {
        com.google.protobuf.Duration duration = this.timeWindow.getLatestLeadDuration();

        return Duration.ofSeconds( duration.getSeconds(), duration.getNanos() );
    }

    /**
     * Returns the underlying state.
     * 
     * @return the canonical state
     */

    public TimeWindow getTimeWindow()
    {
        return this.timeWindow;
    }

    /**
     * Hidden constructor.
     * 
     * @param timeWindow the canonical time window
     * @throws IllegalArgumentException if a right (latest) bookend is earlier than a left (earliest) bookend
     * @throws NullPointerException if the time window is null
     */

    private TimeWindowOuter( TimeWindow timeWindow )
    {
        Objects.requireNonNull( timeWindow, "Cannot build a time window with null input." );

        this.timeWindow = timeWindow;

        if ( !this.getTimeWindow().hasEarliestReferenceTime() )
        {
            throw new NullPointerException( "The earliest reference time cannot be null." );
        }

        if ( !this.getTimeWindow().hasLatestReferenceTime() )
        {
            throw new NullPointerException( "The latest reference time cannot be null." );
        }

        if ( !this.getTimeWindow().hasEarliestValidTime() )
        {
            throw new NullPointerException( "The earliest valid time cannot be null." );
        }

        if ( !this.getTimeWindow().hasLatestValidTime() )
        {
            throw new NullPointerException( "The latest valid time cannot be null." );
        }

        if ( !this.getTimeWindow().hasEarliestLeadDuration() )
        {
            throw new NullPointerException( "The earliest lead duration cannot be null." );
        }

        if ( !this.getTimeWindow().hasLatestLeadDuration() )
        {
            throw new NullPointerException( "The latest lead duration cannot be null." );
        }

        // Correct time ordering
        if ( this.getLatestReferenceTime().isBefore( this.getEarliestReferenceTime() ) )
        {
            throw new IllegalArgumentException( "Cannot define a time window whose latest reference time is "
                                                + "before its earliest reference time." );
        }
        if ( this.getLatestValidTime().isBefore( this.getEarliestValidTime() ) )
        {
            throw new IllegalArgumentException( "Cannot define a time window whose latest valid time is "
                                                + "before its earliest valid time." );
        }
        if ( this.getLatestLeadDuration().compareTo( this.getEarliestLeadDuration() ) < 0 )
        {
            throw new IllegalArgumentException( "Cannot define a time window whose latest lead duration is "
                                                + "before its earliest lead duration." );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Created a new time window: {}.", this );
        }
    }

}
