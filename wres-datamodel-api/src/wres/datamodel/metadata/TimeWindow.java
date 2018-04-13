package wres.datamodel.metadata;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * <p>Metadata that describes the partition of two time lines in which a sample is collated for the purposes of a 
 * statistical calculation. The first timeline is absolute (UTC), and is defined by an earliest time and a latest time 
 * in a specified reference time system. The reference time system is either valid time or forecast issue time. The 
 * second timeline is relative, and comprises an earliest forecast lead time and a latest forecast lead time. A 
 * {@link TimeWindow} represents the intersection of these two timelines, i.e. it contains elements that are common to 
 * (the partition of) each timeline.</p> 
 * 
 * <p>In summary, a {@link TimeWindow} comprises the following required elements:</p>
 * 
 * <ol>
 * <li>The earliest time</li>
 * <li>The latest time</li>
 * <li>The reference time system (valid time or issue time)
 * <li>The earliest forecast lead time</li>
 * <li>The latest forecast lead time</li>
 * </ol>
 * 
 * <p>When describing a sample that does not comprise forecasts, the reference time should be valid time, and the 
 * forecast lead times should be zero.</p>
 * 
 * <p>TODO: If a future JDK implements something equivalent to an Interval in joda.time, consider replacing the 
 * earliest time and latest time with an Interval.</p>
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>This class is immutable and thread-safe.</p>
 * 
 * @version 0.1
 * @since 0.2
 * @author james.brown@hydrosolved.com
 */

public final class TimeWindow implements Comparable<TimeWindow>
{

    /**
     * The earliest time associated with the time window.
     */

    private final Instant earliestTime;

    /**
     * The latest time associated with the time window.
     */

    private final Instant latestTime;

    /**
     * The reference time system for the {@link #earliestTime} and {@link #latestTime}. 
     */

    private final ReferenceTime referenceTime;

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
     * Constructs a {@link TimeWindow}.
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @param referenceTime the reference time for the earliestTime and latestTime
     * @param earliestLead the earliest lead time
     * @param latestLead the latest lead time
     * @return a time window
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime or the 
     *            latestLead is before (i.e. smaller than) the earliestLead
     */

    public static TimeWindow of( Instant earliestTime,
                                 Instant latestTime,
                                 ReferenceTime referenceTime,
                                 Duration earliestLead,
                                 Duration latestLead )
    {
        return new TimeWindow( earliestTime, latestTime, referenceTime, earliestLead, latestLead );
    }

    /**
     * Constructs a {@link TimeWindow} where the {@link #earliestLead} and {@link #latestLead} both 
     * have the same value.
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @param referenceTime the reference time for the earliestTime and latestTime
     * @param lead the earliest and latest lead time
     * @return a time window
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime
     */

    public static TimeWindow of( Instant earliestTime, Instant latestTime, ReferenceTime referenceTime, Duration lead )
    {
        return new TimeWindow( earliestTime, latestTime, referenceTime, lead, lead );
    }

    /**
     * Constructs a {@link TimeWindow} where the {@link #earliestLead} and {@link #latestLead} are both 
     * zero and the {@link #referenceTime} is {@link ReferenceTime#VALID_TIME}.
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @return a time window
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime
     */

    public static TimeWindow of( Instant earliestTime, Instant latestTime )
    {
        return new TimeWindow( earliestTime, latestTime, ReferenceTime.VALID_TIME, Duration.ZERO, Duration.ZERO );
    }

    /**
     * Returns a {@link TimeWindow} that represents the union of the inputs, specifically where the
     * {@link #earliestTime} and {@link #latestTime} and {@link #earliestLead} and {@link #latestLead} are the 
     * earliest and latest instances, respectively.
     * 
     * @param input the input windows
     * @return the union of the inputs with respect to dates and lead times
     * @throws NullPointerException if the input is null
     */

    public static TimeWindow unionOf( List<TimeWindow> input )
    {
        Objects.requireNonNull( input, "Cannot determine the union of time windows for a null input." );
        // Check and set time parameters
        Instant earliestT = null;
        Instant latestT = null;
        Duration earliestL = null;
        Duration latestL = null;
        ReferenceTime referenceTime = null;
        for ( TimeWindow next : input )
        {
            // Initialize
            if ( Objects.isNull( earliestT ) )
            {
                earliestT = next.earliestTime;
                latestT = next.latestTime;
                earliestL = next.earliestLead;
                latestL = next.latestLead;
                referenceTime = next.referenceTime;
            }
            else
            {
                if ( earliestT.isAfter( next.earliestTime ) )
                {
                    earliestT = next.earliestTime;
                }
                if ( latestT.isBefore( next.latestTime ) )
                {
                    latestT = next.latestTime;
                }
                if ( earliestL.compareTo( next.earliestLead ) > 0 )
                {
                    earliestL = next.earliestLead;
                }
                if ( latestL.compareTo( next.latestLead ) < 0 )
                {
                    latestL = next.latestLead;
                }
            }
        }
        return new TimeWindow( earliestT, latestT, referenceTime, earliestL, latestL );
    }    

    @Override
    public int compareTo( TimeWindow o )
    {
        int compare = earliestTime.compareTo( o.earliestTime );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = latestTime.compareTo( o.latestTime );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = referenceTime.compareTo( o.referenceTime );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = earliestLead.compareTo( o.earliestLead );
        if ( compare != 0 )
        {
            return compare;
        }
        return latestLead.compareTo( o.latestLead );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof TimeWindow ) )
        {
            return false;
        }
        TimeWindow in = (TimeWindow) o;
        boolean timesEqual = in.earliestTime.equals( earliestTime ) && in.latestTime.equals( latestTime )
                             && in.referenceTime.equals( referenceTime );
        return timesEqual && earliestLead.equals( in.earliestLead )
               && latestLead.equals( in.latestLead );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( earliestTime,
                             latestTime,
                             referenceTime,
                             earliestLead,
                             latestLead );
    }

    @Override
    public String toString()
    {
        StringJoiner sj = new StringJoiner( ", ", "[", "]" );
        sj.add( earliestTime.toString() )
          .add( latestTime.toString() )
          .add( referenceTime.toString() )
          .add( earliestLead.toString() )
          .add( latestLead.toString() );
        return sj.toString();
    }

    /**
     * Returns the earliest {@link Instant} associated with the time window
     * 
     * @return the earliest instant
     */

    public Instant getEarliestTime()
    {
        return earliestTime;
    }

    /**
     * Returns the latest {@link Instant} associated with the time window
     * 
     * @return the latest instant
     */

    public Instant getLatestTime()
    {
        return latestTime;
    }

    /**
     * Returns <code>true</code> if {@link #getEarliestTime()} returns {@link Instant#MIN} or 
     * {@link #getLatestTime()} returns {@link Instant#MAX}, false otherwise.
     * 
     * @return true if the timeline is unbounded, false otherwise
     */

    public boolean hasUnboundedTimes()
    {
        return earliestTime.equals( Instant.MIN ) || latestTime.equals( Instant.MAX );
    }

    /**
     * Returns the mid-point on the UTC timeline between the {@link #earliestTime} and the {@link #latestTime}.
     * 
     * @return the mid-point on the UTC timeline
     */

    public Instant getMidPointTime()
    {
        return earliestTime.plus( getDuration().dividedBy( 2 ) );
    }

    /**
     * Returns the {@link Duration} between the {@link #earliestTime} and the {@link #latestTime}.
     * 
     * @return the duration between the earliest and latest times
     */

    public Duration getDuration()
    {
        return Duration.between( earliestTime, latestTime );
    }

    /**
     * Returns the reference time system for the {@link earliestTime} and {@link latestTime}.
     * 
     * @return the reference time system
     */

    public ReferenceTime getReferenceTime()
    {
        return referenceTime;
    }

    /**
     * Returns the earliest forecast lead time.
     * 
     * @return the earliest lead time
     */

    public Duration getEarliestLeadTime()
    {
        return earliestLead;
    }

    /**
     * Returns the latest forecast lead time.
     * 
     * @return the latest lead time
     */

    public Duration getLatestLeadTime()
    {
        return latestLead;
    }

    /**
     * A convenience method that returns the earliest forecast lead time in hours.
     * 
     * @return the earliest lead time in hours
     */

    public long getEarliestLeadTimeInHours()
    {
        return earliestLead.toHours();
    }

    /**
     * A convenience method that returns the latest forecast lead time in hours.
     * 
     * @return the latest lead time in hours
     */

    public long getLatestLeadTimeInHours()
    {
        return latestLead.toHours();
    }

    /**
     * A convenience method that returns the earliest forecast lead time in seconds.
     * 
     * @return the earliest lead time in seconds
     */

    public long getEarliestLeadTimeInSeconds()
    {
        return earliestLead.getSeconds();
    }

    /**
     * A convenience method that returns the latest forecast lead time in seconds.
     * 
     * @return the latest lead time in seconds
     */

    public long getLatestLeadTimeInSeconds()
    {
        return latestLead.getSeconds();
    }

    /**
     * Hidden constructor.
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @param referenceTime the reference time
     * @param earliestLead the earliest lead time
     * @param latestLead the latest lead time
     * @param leadUnits the units of the lead time
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime or the 
     *            latestLeadTime is before (i.e. smaller than) the earliestLeadTime.  
     */

    private TimeWindow( Instant earliestTime,
                        Instant latestTime,
                        ReferenceTime referenceTime,
                        Duration earliestLead,
                        Duration latestLead )
    {
        if ( latestTime.isBefore( earliestTime ) )
        {
            throw new IllegalArgumentException( "Cannot define a time window whose latest time is before its "
                                                + "earliest time." );
        }
        if ( latestLead.compareTo( earliestLead ) < 0 )
        {
            throw new IllegalArgumentException( "Cannot define a time window whose latest lead time is before its "
                                                + "earliest lead time." );
        }
        this.earliestTime = earliestTime;
        this.latestTime = latestTime;
        this.referenceTime = referenceTime;
        this.earliestLead = earliestLead;
        this.latestLead = latestLead;
    }

}
