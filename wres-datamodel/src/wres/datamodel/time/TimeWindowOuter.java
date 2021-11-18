package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.protobuf.Timestamp;

import wres.statistics.generated.TimeWindow;


/**
 * <p>Metadata that describes the partition of three time lines in which a sample is collated for the purposes of a 
 * statistical calculation. The first timeline is a reference timeline in UTC, which may be used to represent
 * the origin of a forecast (e.g. the issued datetime). The second timeline represents the valid datetime of a 
 * measurement, also in UTC. The third timeline describes the duration of a measurement, such as a forecast 
 * lead duration. Each timeline is bounded with an earliest and latest bookend. A {@link TimeWindowOuter} represents 
 * the intersection of these three timelines, i.e. each of its elements are members of each of the three 
 * timelines.</p> 
 * 
 * <p>In summary, a {@link TimeWindowOuter} comprises the following required elements:</p>
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
 * <p><b>Implementation Requirements:</b></p>
 *  
 * <p>This class is immutable and thread-safe.</p>
 * 
 * <p>TODO: If a future JDK implements something equivalent to an Interval in joda.time, consider replacing the 
 * earliest time and latest time with an Interval.</p>
 * 
 * <p>The internal data is stored, and accessible, as a {@link TimeWindow}.
 *
 * @author James Brown
 */

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

    /**
     * The internal state.
     */

    private final TimeWindow timeWindow;

    /**
     * Constructs a {@link TimeWindowOuter} where the earliest lead durations is {@link TimeWindowOuter#DURATION_MIN}, the 
     * latest lead duration is {@link TimeWindowOuter#DURATION_MAX}, the earliest valid time is {@link Instant#MIN}, the 
     * latest valid time is {@link Instant#MAX}, the earliest reference time is {@link Instant#MIN} and the 
     * latest reference time is {@link Instant#MAX}.
     * 
     * @return a time window
     */

    public static TimeWindowOuter of()
    {
        return new Builder().build();
    }

    /**
     * Constructs a {@link TimeWindowOuter} with a canonical {@link TimeWindow}.
     * 
     * @param timeWindow a time window
     * @return a time window
     */

    public static TimeWindowOuter of( TimeWindow timeWindow )
    {
        return new Builder( timeWindow ).build();
    }

    /**
     * Constructs a {@link TimeWindowOuter} where the earliest lead durations is {@link TimeWindowOuter#DURATION_MIN}, the 
     * latest lead duration is {@link TimeWindowOuter#DURATION_MAX}, the earliest reference time is {@link Instant#MIN}, the 
     * latest reference time is {@link Instant#MAX}, and the valid times are given.
     * 
     * @param earliestValidTime the earliest time
     * @param latestValidTime the latest time
     * @return a time window
     * @throws IllegalArgumentException if the latestValidTime is before the earliestValidTime
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter of( Instant earliestValidTime, Instant latestValidTime )
    {
        return new Builder().setEarliestValidTime( earliestValidTime )
                            .setLatestValidTime( latestValidTime )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindowOuter} where the earliest reference time is {@link Instant#MIN}, the latest reference 
     * time is {@link Instant#MAX}, the earliest valid time is {@link Instant#MIN}, and the latest valid time 
     * is {@link Instant#MAX}.
     * 
     * @param earliestLead the earliest lead duration
     * @param latestLead the latest lead duration
     * @return a time window
     * @throws IllegalArgumentException if the latestLead is smaller than the earliestLead
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter of( Duration earliestLead,
                                      Duration latestLead )
    {
        return new Builder().setEarliestLeadDuration( earliestLead )
                            .setLatestLeadDuration( latestLead )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindowOuter} where the earliest lead durations is {@link TimeWindowOuter#DURATION_MIN}, the 
     * latest lead duration is {@link TimeWindowOuter#DURATION_MAX}, and the other components are given.
     * 
     * @param earliestReferenceTime the earliest reference time
     * @param latestReferenceTime the latest reference time
     * @param earliestValidTime the earliest valid time
     * @param latestValidTime the latest valid time
     * @return a time window
     * @throws IllegalArgumentException if the latestReferenceTime is before the earliestReferenceTime 
     *            or the latestValidTime is before the earliestValidTime
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter of( Instant earliestReferenceTime,
                                      Instant latestReferenceTime,
                                      Instant earliestValidTime,
                                      Instant latestValidTime )
    {
        return new Builder().setEarliestReferenceTime( earliestReferenceTime )
                            .setLatestReferenceTime( latestReferenceTime )
                            .setEarliestValidTime( earliestValidTime )
                            .setLatestValidTime( latestValidTime )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindowOuter} where the {@link #getEarliestLeadDuration()} and 
     * {@link #getLatestLeadDuration()} both have the same value, the earliest valid time is {@link Instant#MIN} and 
     * the latest valid time is {@link Instant#MAX}.
     * 
     * @param earliestReferenceTime the earliest reference time
     * @param latestReferenceTime the latest reference time
     * @param lead the earliest and latest lead duration
     * @return a time window
     * @throws IllegalArgumentException if the latestReferenceTime is before the earliestReferenceTime
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter of( Instant earliestReferenceTime, Instant latestReferenceTime, Duration lead )
    {
        return new Builder().setEarliestReferenceTime( earliestReferenceTime )
                            .setLatestReferenceTime( latestReferenceTime )
                            .setEarliestLeadDuration( lead )
                            .setLatestLeadDuration( lead )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindowOuter} where the earliest valid time is {@link Instant#MIN} and the latest valid time 
     * is {@link Instant#MAX}.
     * 
     * @param earliestReferenceTime the earliest reference time
     * @param latestReferenceTime the latest reference time
     * @param earliestLead the earliest lead duration
     * @param latestLead the latest lead duration
     * @return a time window
     * @throws IllegalArgumentException if the latestReferenceTime is before the earliestReferenceTime or the 
     *            latestLead is smaller than the earliestLead
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter of( Instant earliestReferenceTime,
                                      Instant latestReferenceTime,
                                      Duration earliestLead,
                                      Duration latestLead )
    {
        return new Builder().setEarliestReferenceTime( earliestReferenceTime )
                            .setLatestReferenceTime( latestReferenceTime )
                            .setEarliestLeadDuration( earliestLead )
                            .setLatestLeadDuration( latestLead )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindowOuter}.
     * 
     * @param earliestReferenceTime the earliest reference time
     * @param latestReferenceTime the latest reference time
     * @param earliestValidTime the earliest valid time
     * @param latestValidTime the latest valid time
     * @param earliestLead the earliest lead duration
     * @param latestLead the latest lead duration
     * @return a time window
     * @throws IllegalArgumentException if the latestReferenceTime is before the earliestReferenceTime or the 
     *            latestValidTime is before the earliestValidTime or the latestLead is smaller than the earliestLead
     * @throws NullPointerException if any input is null
     */

    public static TimeWindowOuter of( Instant earliestReferenceTime,
                                      Instant latestReferenceTime,
                                      Instant earliestValidTime,
                                      Instant latestValidTime,
                                      Duration earliestLead,
                                      Duration latestLead )
    {
        return new Builder().setEarliestReferenceTime( earliestReferenceTime )
                            .setLatestReferenceTime( latestReferenceTime )
                            .setEarliestValidTime( earliestValidTime )
                            .setLatestValidTime( latestValidTime )
                            .setEarliestLeadDuration( earliestLead )
                            .setLatestLeadDuration( latestLead )
                            .build();
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

        return TimeWindowOuter.of( earliestR, latestR, earliestV, latestV, earliestL, latestL );
    }

    /**
     * @return a builder with the same content as the current {@link TimeWindowOuter}.
     */

    public Builder toBuilder()
    {
        return new TimeWindowOuter.Builder( this.getTimeWindow() );
    }

    @Override
    public int compareTo( TimeWindowOuter o )
    {
        int compare = this.getEarliestReferenceTime().compareTo( o.getEarliestReferenceTime() );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = this.getLatestReferenceTime().compareTo( o.getLatestReferenceTime() );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = this.getEarliestValidTime().compareTo( o.getEarliestValidTime() );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = this.getLatestValidTime().compareTo( o.getLatestValidTime() );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = this.getEarliestLeadDuration().compareTo( o.getEarliestLeadDuration() );
        if ( compare != 0 )
        {
            return compare;
        }

        return this.getLatestLeadDuration().compareTo( o.getLatestLeadDuration() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof TimeWindowOuter ) )
        {
            return false;
        }
        TimeWindowOuter in = (TimeWindowOuter) o;
        boolean timesEqual = in.getEarliestReferenceTime().equals( this.getEarliestReferenceTime() )
                             && in.getLatestReferenceTime().equals( this.getLatestReferenceTime() )
                             && in.getEarliestValidTime().equals( this.getEarliestValidTime() )
                             && in.getLatestValidTime().equals( this.getLatestValidTime() );
        return timesEqual && in.getEarliestLeadDuration().equals( this.getEarliestLeadDuration() )
               && in.getLatestLeadDuration().equals( this.getLatestLeadDuration() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getEarliestReferenceTime(),
                             this.getLatestReferenceTime(),
                             this.getEarliestValidTime(),
                             this.getLatestValidTime(),
                             this.getEarliestLeadDuration(),
                             this.getLatestLeadDuration() );
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
     * Build an {@link TimeWindowOuter} incrementally. 
     */

    public static class Builder
    {

        /**
         * The earliest time associated with the time window.
         */

        private Instant earliestReferenceTime = Instant.MIN;

        /**
         * The latest time associated with the time window.
         */

        private Instant latestReferenceTime = Instant.MAX;

        /**
         * The earliest valid time associated with the time window.
         */

        private Instant earliestValidTime = Instant.MIN;

        /**
         * The latest valid time associated with the time window.
         */

        private Instant latestValidTime = Instant.MAX;

        /**
         * The earliest forecast lead time.
         */

        private Duration earliestLead = TimeWindowOuter.DURATION_MIN;

        /**
         * The latest forecast lead time. 
         */

        private Duration latestLead = TimeWindowOuter.DURATION_MAX;

        /**
         * Sets the earliest reference time.
         * 
         * @param earliestReferenceTime the earliest reference time
         * @return the builder
         */

        public Builder setEarliestReferenceTime( Instant earliestReferenceTime )
        {
            this.earliestReferenceTime = earliestReferenceTime;
            return this;
        }

        /**
         * Sets the latest reference time.
         * 
         * @param latestReferenceTime the latest reference time
         * @return the builder
         */

        public Builder setLatestReferenceTime( Instant latestReferenceTime )
        {
            this.latestReferenceTime = latestReferenceTime;
            return this;
        }

        /**
         * Sets the earliest valid time.
         * 
         * @param earliestValidTime the earliest valid time
         * @return the builder
         */

        public Builder setEarliestValidTime( Instant earliestValidTime )
        {
            this.earliestValidTime = earliestValidTime;
            return this;
        }

        /**
         * Sets the latest valid time.
         * 
         * @param latestValidTime the latest valid time
         * @return the builder
         */

        public Builder setLatestValidTime( Instant latestValidTime )
        {
            this.latestValidTime = latestValidTime;
            return this;
        }

        /**
         * Sets the earliest lead time.
         * 
         * @param earliestLead the earliest lead time
         * @return the builder
         */

        public Builder setEarliestLeadDuration( Duration earliestLead )
        {
            this.earliestLead = earliestLead;
            return this;
        }

        /**
         * Sets the latest lead time.
         * 
         * @param latestLead the latest lead time
         * @return the builder
         */

        public Builder setLatestLeadDuration( Duration latestLead )
        {
            this.latestLead = latestLead;
            return this;
        }

        /**
         * Returns an instance.
         * 
         * @return an instance of a {@link TimeWindowOuter}
         */

        public TimeWindowOuter build()
        {
            return new TimeWindowOuter( this );
        }

        /**
         * Default constructor.
         */

        public Builder()
        {
        }

        /**
         * Constructs with a canonical instance.
         * 
         * @param timeWindow the time window.
         */

        public Builder( TimeWindow timeWindow )
        {
            this.earliestLead = Duration.ofSeconds( timeWindow.getEarliestLeadDuration().getSeconds(),
                                                    timeWindow.getEarliestLeadDuration().getNanos() );
            this.latestLead = Duration.ofSeconds( timeWindow.getLatestLeadDuration().getSeconds(),
                                                  timeWindow.getLatestLeadDuration().getNanos() );
            this.earliestReferenceTime = Instant.ofEpochSecond( timeWindow.getEarliestReferenceTime().getSeconds(),
                                                                timeWindow.getEarliestReferenceTime().getNanos() );
            this.latestReferenceTime = Instant.ofEpochSecond( timeWindow.getLatestReferenceTime().getSeconds(),
                                                              timeWindow.getLatestReferenceTime().getNanos() );
            this.earliestValidTime = Instant.ofEpochSecond( timeWindow.getEarliestValidTime().getSeconds(),
                                                            timeWindow.getEarliestValidTime().getNanos() );
            this.latestValidTime = Instant.ofEpochSecond( timeWindow.getLatestValidTime().getSeconds(),
                                                          timeWindow.getLatestValidTime().getNanos() );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws IllegalArgumentException if a right (latest) bookend is earlier than a left (earliest) bookend.  
     * @throws NullPointerException if the earliestTime or latestTime are null
     */

    private TimeWindowOuter( Builder builder )
    {
        // Set then validate
        Instant earliestReferenceTime = builder.earliestReferenceTime;
        Instant latestReferenceTime = builder.latestReferenceTime;
        Instant earliestValidTime = builder.earliestValidTime;
        Instant latestValidTime = builder.latestValidTime;
        Duration earliestLead = builder.earliestLead;
        Duration latestLead = builder.latestLead;
        TimeWindow.Builder timeWindowBuilder = TimeWindow.newBuilder();

        if ( Objects.nonNull( earliestReferenceTime ) )
        {
            timeWindowBuilder.setEarliestReferenceTime( Timestamp.newBuilder()
                                                                 .setSeconds( earliestReferenceTime.getEpochSecond() )
                                                                 .setNanos( earliestReferenceTime.getNano() ) );
        }

        if ( Objects.nonNull( latestReferenceTime ) )
        {
            timeWindowBuilder.setLatestReferenceTime( Timestamp.newBuilder()
                                                               .setSeconds( latestReferenceTime.getEpochSecond() )
                                                               .setNanos( latestReferenceTime.getNano() ) );
        }

        if ( Objects.nonNull( earliestValidTime ) )
        {
            timeWindowBuilder.setEarliestValidTime( Timestamp.newBuilder()
                                                             .setSeconds( earliestValidTime.getEpochSecond() )
                                                             .setNanos( earliestValidTime.getNano() ) );
        }

        if ( Objects.nonNull( latestValidTime ) )
        {
            timeWindowBuilder.setLatestValidTime( Timestamp.newBuilder()
                                                           .setSeconds( latestValidTime.getEpochSecond() )
                                                           .setNanos( latestValidTime.getNano() ) );
        }

        if ( Objects.nonNull( earliestLead ) )
        {
            timeWindowBuilder.setEarliestLeadDuration( com.google.protobuf.Duration.newBuilder()
                                                                                   .setSeconds( earliestLead.getSeconds() )
                                                                                   .setNanos( earliestLead.getNano() ) );
        }

        if ( Objects.nonNull( latestLead ) )
        {
            timeWindowBuilder.setLatestLeadDuration( com.google.protobuf.Duration.newBuilder()
                                                                                 .setSeconds( latestLead.getSeconds() )
                                                                                 .setNanos( latestLead.getNano() ) );
        }

        this.timeWindow = timeWindowBuilder.build();

        // Validate
        this.validate();
    }

    /**
     * Validates the preliminary state and throws an exception if invalid.
     * 
     * @throws NullPointerException if any component of the time window is null
     * @throws IllegalArgumentException if any component of the time window is internally inconsistent
     */

    private void validate()
    {
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
    }

}
