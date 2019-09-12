package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * <p>Metadata that describes the partition of three time lines in which a sample is collated for the purposes of a 
 * statistical calculation. The first timeline is a reference timeline in UTC, which may be used to represent
 * the origin of a forecast (e.g. the issued datetime). The second timeline represents the valid datetime of a 
 * measurement, also in UTC. The third timeline describes the duration of a measurement, such as a forecast 
 * lead duration. Each timeline is bounded with an earliest and latest bookend. A {@link TimeWindow} represents 
 * the intersection of these three timelines, i.e. each of its elements are members of each of the three 
 * timelines.</p> 
 * 
 * <p>In summary, a {@link TimeWindow} comprises the following required elements:</p>
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
 * @author james.brown@hydrosolved.com
 */

public final class TimeWindow implements Comparable<TimeWindow>
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
     * The earliest reference time associated with the time window.
     */

    private final Instant earliestReferenceTime;

    /**
     * The latest reference time associated with the time window.
     */

    private final Instant latestReferenceTime;

    /**
     * The earliest valid time associated with the time window.
     */

    private final Instant earliestValidTime;

    /**
     * The latest valid time associated with the time window.
     */

    private final Instant latestValidTime;

    /**
     * The earliest forecast lead time associated with the time window in {@link leadUnits} units. If the 
     * {@link TimeWindow} does not represent a forecast, the {@link #earliestLead} and {@link #latestLead} should 
     * both be zero.
     */

    private final Duration earliestLead;

    /**
     * The latest forecast lead time associated with the time window. If the {@link TimeWindow} does not represent a 
     * forecast, the {@link #earliestLead} and {@link #latestLead} should both be zero. 
     */

    private final Duration latestLead;

    /**
     * Constructs a {@link TimeWindow} where the earliest lead durations is {@link TimeWindow#DURATION_MIN}, the 
     * latest lead duration is {@link TimeWindow#DURATION_MAX}, the earliest valid time is {@link Instant#MIN}, the 
     * latest valid time is {@link Instant#MAX}, the earliest reference time is {@link Instant#MIN} and the 
     * latest reference time is {@link Instant#MAX}.
     * 
     * @return a time window
     */

    public static TimeWindow of()
    {
        return new Builder().setEarliestReferenceTime( Instant.MIN )
                            .setLatestReferenceTime( Instant.MAX )
                            .setEarliestValidTime( Instant.MIN )
                            .setLatestValidTime( Instant.MAX )
                            .setEarliestLeadDuration( TimeWindow.DURATION_MIN )
                            .setLatestLeadDuration( TimeWindow.DURATION_MAX )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindow} where the earliest lead durations is {@link TimeWindow#DURATION_MIN}, the 
     * latest lead duration is {@link TimeWindow#DURATION_MAX}, the earliest reference time is {@link Instant#MIN}, the 
     * latest reference time is {@link Instant#MAX}, and the valid times are given.
     * 
     * @param earliestValidTime the earliest time
     * @param latestValidTime the latest time
     * @return a time window
     * @throws IllegalArgumentException if the latestValidTime is before the earliestValidTime
     * @throws NullPointerException if any input is null
     */

    public static TimeWindow of( Instant earliestValidTime, Instant latestValidTime )
    {
        return new Builder().setEarliestValidTime( earliestValidTime )
                            .setLatestValidTime( latestValidTime )
                            .setEarliestReferenceTime( Instant.MIN )
                            .setLatestReferenceTime( Instant.MAX )
                            .setEarliestLeadDuration( TimeWindow.DURATION_MIN )
                            .setLatestLeadDuration( TimeWindow.DURATION_MAX )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindow} where the earliest reference time is {@link Instant#MIN}, the latest reference 
     * time is {@link Instant#MAX}, the earliest valid time is {@link Instant#MIN}, and the latest valid time 
     * is {@link Instant#MAX}.
     * 
     * @param earliestLead the earliest lead duration
     * @param latestLead the latest lead duration
     * @return a time window
     * @throws IllegalArgumentException if the latestLead is smaller than the earliestLead
     * @throws NullPointerException if any input is null
     */

    public static TimeWindow of( Duration earliestLead,
                                 Duration latestLead )
    {
        return new Builder().setEarliestReferenceTime( Instant.MIN )
                            .setLatestReferenceTime( Instant.MAX )
                            .setEarliestValidTime( Instant.MIN )
                            .setLatestValidTime( Instant.MAX )
                            .setEarliestLeadDuration( earliestLead )
                            .setLatestLeadDuration( latestLead )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindow} where the earliest lead durations is {@link TimeWindow#DURATION_MIN}, the 
     * latest lead duration is {@link TimeWindow#DURATION_MAX}, and the other components are given.
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

    public static TimeWindow of( Instant earliestReferenceTime,
                                 Instant latestReferenceTime,
                                 Instant earliestValidTime,
                                 Instant latestValidTime )
    {
        return new Builder().setEarliestReferenceTime( earliestReferenceTime )
                            .setLatestReferenceTime( latestReferenceTime )
                            .setEarliestValidTime( earliestValidTime )
                            .setLatestValidTime( latestValidTime )
                            .setEarliestLeadDuration( TimeWindow.DURATION_MIN )
                            .setLatestLeadDuration( TimeWindow.DURATION_MAX )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindow} where the {@link #earliestLead} and {@link #latestLead} both 
     * have the same value, the earliest valid time is {@link Instant#MIN} and the latest valid time is 
     * {@link Instant#MAX}.
     * 
     * @param earliestReferenceTime the earliest reference time
     * @param latestReferenceTime the latest reference time
     * @param lead the earliest and latest lead duration
     * @return a time window
     * @throws IllegalArgumentException if the latestReferenceTime is before the earliestReferenceTime
     * @throws NullPointerException if any input is null
     */

    public static TimeWindow of( Instant earliestReferenceTime, Instant latestReferenceTime, Duration lead )
    {
        return new Builder().setEarliestReferenceTime( earliestReferenceTime )
                            .setLatestReferenceTime( latestReferenceTime )
                            .setEarliestValidTime( Instant.MIN )
                            .setLatestValidTime( Instant.MAX )
                            .setEarliestLeadDuration( lead )
                            .setLatestLeadDuration( lead )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindow} where the earliest valid time is {@link Instant#MIN} and the latest valid time 
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

    public static TimeWindow of( Instant earliestReferenceTime,
                                 Instant latestReferenceTime,
                                 Duration earliestLead,
                                 Duration latestLead )
    {
        return new Builder().setEarliestReferenceTime( earliestReferenceTime )
                            .setLatestReferenceTime( latestReferenceTime )
                            .setEarliestValidTime( Instant.MIN )
                            .setLatestValidTime( Instant.MAX )
                            .setEarliestLeadDuration( earliestLead )
                            .setLatestLeadDuration( latestLead )
                            .build();
    }

    /**
     * Constructs a {@link TimeWindow}.
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

    public static TimeWindow of( Instant earliestReferenceTime,
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
     * Returns a {@link TimeWindow} that represents the union of the inputs, specifically where the
     * {@link #earliestReferenceTime} and {@link #latestReferenceTime} are the earliest and latest instances, 
     * respectively, and likewise for the {@link #earliestValidTime} and {@link #latestValidTime}, and the 
     * {@link #earliestLead} and {@link #latestLead}.
     * 
     * @param input the input windows
     * @return the union of the inputs with respect to dates and lead times
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if any input is null
     */

    public static TimeWindow unionOf( List<TimeWindow> input )
    {
        Objects.requireNonNull( input, "Cannot determine the union of time windows for a null input." );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot determine the union of time windows for empty input." );
        }

        if ( input.contains( null ) )
        {
            throw new IllegalArgumentException( "Cannot determine the union of time windows for input that contains "
                                                + "one or more null time windows." );
        }

        // Check and set time parameters
        Instant earliestR = input.get( 0 ).getEarliestReferenceTime();
        Instant latestR = input.get( 0 ).getLatestReferenceTime();
        Instant earliestV = input.get( 0 ).getEarliestValidTime();
        Instant latestV = input.get( 0 ).getLatestValidTime();
        Duration earliestL = input.get( 0 ).getEarliestLeadDuration();
        Duration latestL = input.get( 0 ).getLatestLeadDuration();

        for ( TimeWindow next : input )
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

        return TimeWindow.of( earliestR, latestR, earliestV, latestV, earliestL, latestL );
    }

    @Override
    public int compareTo( TimeWindow o )
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
        if ( ! ( o instanceof TimeWindow ) )
        {
            return false;
        }
        TimeWindow in = (TimeWindow) o;
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
        StringJoiner sj = new StringJoiner( ",", "[", "]" );
        sj.add( this.getEarliestReferenceTime().toString() )
          .add( this.getLatestReferenceTime().toString() )
          .add( this.getEarliestValidTime().toString() )
          .add( this.getLatestValidTime().toString() )
          .add( this.getEarliestLeadDuration().toString() )
          .add( this.getLatestLeadDuration().toString() );
        return sj.toString();
    }

    /**
     * Returns the earliest reference time {@link Instant} associated with the time window
     * 
     * @return the earliest reference time instant
     */

    public Instant getEarliestReferenceTime()
    {
        return this.earliestReferenceTime;
    }

    /**
     * Returns the latest reference time {@link Instant} associated with the time window
     * 
     * @return the latest reference time instant
     */

    public Instant getLatestReferenceTime()
    {
        return this.latestReferenceTime;
    }

    /**
     * Returns the earliest valid time {@link Instant} associated with the time window
     * 
     * @return the earliest valid time instant
     */

    public Instant getEarliestValidTime()
    {
        return this.earliestValidTime;
    }

    /**
     * Returns the latest valid time {@link Instant} associated with the time window
     * 
     * @return the latest valid time instant
     */

    public Instant getLatestValidTime()
    {
        return this.latestValidTime;
    }

    /**
     * Returns the mid-point on the UTC timeline between the {@link #earliestReferenceTime} and the {@link #latestReferenceTime}.
     * 
     * @return the mid-point on the UTC timeline
     */

    public Instant getMidPointBetweenEarliestAndLatestReferenceTimes()
    {
        return this.getEarliestReferenceTime()
                   .plus( Duration.between( this.getEarliestReferenceTime(), this.getLatestReferenceTime() )
                                  .dividedBy( 2 ) );
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
     * Returns <code>true</code> if {@link #getEarliestLeadDuration()} returns {@link TimeWindow#DURATION_MIN} and 
     * {@link #getLatestLeadDuration()} returns {@link TimeWindow#DURATION_MIN}, otherwise <code>false</code>.
     * 
     * @return true if the timeline is unbounded, false otherwise
     */

    public boolean bothLeadDurationsAreUnbounded()
    {
        return TimeWindow.DURATION_MIN.equals( this.getEarliestLeadDuration() )
               && TimeWindow.DURATION_MAX.equals( this.getLatestLeadDuration() );
    }

    /**
     * Returns the earliest forecast lead time.
     * 
     * @return the earliest lead time
     */

    public Duration getEarliestLeadDuration()
    {
        return this.earliestLead;
    }

    /**
     * Returns the latest forecast lead time.
     * 
     * @return the latest lead time
     */

    public Duration getLatestLeadDuration()
    {
        return this.latestLead;
    }

    /**
     * Build an {@link TimeWindow} incrementally. 
     */

    public static class Builder
    {

        /**
         * The earliest time associated with the time window.
         */

        private Instant earliestReferenceTime;

        /**
         * The latest time associated with the time window.
         */

        private Instant latestReferenceTime;

        /**
         * The earliest valid time associated with the time window.
         */

        private Instant earliestValidTime;

        /**
         * The latest valid time associated with the time window.
         */

        private Instant latestValidTime;

        /**
         * The earliest forecast lead time.
         */

        private Duration earliestLead;

        /**
         * The latest forecast lead time. 
         */

        private Duration latestLead;

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
         * @return an instance of a {@link TimeWindow}
         */

        public TimeWindow build()
        {
            return new TimeWindow( this );
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime or the 
     *            latestLeadTime is before (i.e. smaller than) the earliestLeadTime.  
     * @throws NullPointerException if the earliestTime or latestTime are null
     */

    private TimeWindow( Builder builder )
    {
        // Set then validate
        this.earliestReferenceTime = builder.earliestReferenceTime;
        this.latestReferenceTime = builder.latestReferenceTime;
        this.earliestValidTime = builder.earliestValidTime;
        this.latestValidTime = builder.latestValidTime;
        this.earliestLead = builder.earliestLead;
        this.latestLead = builder.latestLead;

        // Validate        
        Objects.requireNonNull( this.getEarliestReferenceTime(), "The earliest reference time cannot be null." );

        Objects.requireNonNull( this.getLatestReferenceTime(), "The latest reference time cannot be null." );

        Objects.requireNonNull( this.getEarliestValidTime(), "The earliest valid time cannot be null." );

        Objects.requireNonNull( this.getLatestValidTime(), "The latest valid time cannot be null." );

        Objects.requireNonNull( this.getEarliestLeadDuration(), "The earliest lead duration cannot be null." );

        Objects.requireNonNull( this.getLatestLeadDuration(), "The latest lead duration cannot be null." );

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
