package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * <p>A partition of two time lines in which a sample is collated for the purposes of a statistical calculation. The 
 * first timeline is absolute (UTC), and is defined by an earliest time and a latest time in a specified reference time 
 * system. The reference time system is either valid time or forecast issue time. The second timeline is relative, 
 * and comprises an earliest forecast lead time and a latest forecast lead time. A {@link TimeWindow} represents the 
 * intersection of these two timelines, i.e. it contains elements that are common to (the partition of) each timeline.</p> 
 * 
 * <p>In summary, a {@link TimeWindow} comprises the following required elements:</p>
 * 
 * <ol>
 * <li>The earliest time</li>
 * <li>The latest time</li>
 * <li>The reference time system (valid time or issue time)
 * <li>The earliest forecast lead time (in seconds)</li>
 * <li>The latest forecast lead time (in seconds)</li>
 * </ol>
 * 
 * <p>When describing a sample that does not comprise forecasts, the reference time should be valid time, and the 
 * forecast lead times should be zero.</p>
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
     * Is true if the reference time for the {@link #earliestTime} and {@link #latestTime} is valid time (i.e. when an
     * event occurs), and false is the reference time is forecast issue time (i.e. the basis time for a forecast). 
     */

    private final boolean validTime;

    /**
     * The earliest forecast lead time, in seconds, associated with the time window. If the {@link TimeWindow} does not 
     * represent a forecast, the {@link #earliestLeadSeconds} and {@link #latestLeadSeconds} should both be zero. 
     * given in seconds.
     */

    private final int earliestLeadSeconds;

    /**
     * The latest forecast lead time, in seconds, associated with the time window. If the {@link TimeWindow} does not 
     * represent a forecast, the {@link #earliestLeadSeconds} and {@link #latestLeadSeconds} should both be zero.  
     */

    private final int latestLeadSeconds;

    /**
     * Constructs a {@link TimeWindow}.
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @param validTime is true if the earliestTime and latestTime are in valid time, false for issue time
     * @param earliestLeadSeconds the earliest lead time in seconds
     * @param latestLeadSeconds the latest lead time in seconds
     * @return a time window
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime or the 
     *            latestLeadTime is before (i.e. smaller than) the earliestLeadTime
     */

    public static TimeWindow of( Instant earliestTime,
                                 Instant latestTime,
                                 boolean validTime,
                                 int earliestLeadSeconds,
                                 int latestLeadSeconds )
    {
        return new TimeWindow( earliestTime, latestTime, validTime, earliestLeadSeconds, latestLeadSeconds );
    }

    /**
     * Constructs a {@link TimeWindow} where the {@link #earliestLeadSeconds} and {@link #latestLeadSeconds} are both 
     * zero and the {@link #validTime} is <code>true</code>.
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @return a time window
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime
     */

    public static TimeWindow of( Instant earliestTime, Instant latestTime )
    {
        return new TimeWindow( earliestTime, latestTime, true, 0, 0 );
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
        compare = Boolean.compare( validTime, o.validTime );
        if ( compare != 0 )
        {
            return compare;
        }
        compare = Long.compare( earliestLeadSeconds, o.earliestLeadSeconds );
        if ( compare != 0 )
        {
            return compare;
        }
        return Long.compare( latestLeadSeconds, o.latestLeadSeconds );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof TimeWindow ) )
        {
            return false;
        }
        TimeWindow in = (TimeWindow) o;
        return in.earliestTime.equals( earliestTime ) && in.latestTime.equals( latestTime )
               && in.validTime == validTime
               && in.earliestLeadSeconds == earliestLeadSeconds
               && in.latestLeadSeconds == latestLeadSeconds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( earliestTime,
                             latestTime,
                             validTime,
                             Integer.hashCode( earliestLeadSeconds ),
                             Integer.hashCode( latestLeadSeconds ) );
    }

    @Override
    public String toString()
    {
        StringJoiner sj = new StringJoiner( ", ", "[", "]" );
        sj.add( OffsetDateTime.ofInstant( earliestTime, ZoneId.of( "UTC" ) ).toString() )
          .add( OffsetDateTime.ofInstant( latestTime, ZoneId.of( "UTC" ) ).toString() )
          .add( Boolean.toString( validTime ) )
          .add( Integer.toString( earliestLeadSeconds ) )
          .add( Integer.toString( latestLeadSeconds ) );
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
     * Returns the mid-point on the UTC timeline between the {@link #earliestTime} and the {@link #latestTime}.
     * 
     * @return the mid-point on the UTC timeline
     */

    public Instant getMidPointTime()
    {
        return earliestTime.plus( Duration.between( earliestTime, latestTime ).dividedBy( 2 ) );
    }

    /**
     * Returns true if the reference time system for the {@link earliestTime} and {@link latestTime} is valid time, 
     * false if it represents forecast issue time.
     * 
     * @return true if the times and valid times, false for issue times
     */

    public boolean isValidTime()
    {
        return validTime;
    }

    /**
     * Returns the earliest forecast lead time in seconds associated with the time window or zero if the time window 
     * does not describe forecasts.
     * 
     * @return the earliest lead time
     */

    public int getEarliestLeadTimeInSeconds()
    {
        return earliestLeadSeconds;
    }

    /**
     * Returns the latest forecast lead time in seconds associated with the time window or zero if the time window 
     * does not describe forecasts.
     * 
     * @return the latest lead time
     */

    public int getLatestLeadTimeInSeconds()
    {
        return latestLeadSeconds;
    }

    /**
     * A convenience method that returns the earliest forecast lead time in decimal hours. See also 
     * {@link #getEarliestLeadTimeInSeconds()}.
     * 
     * @return the earliest lead time in decimal hours
     */

    public double getEarliestLeadTimeInDecimalHours()
    {
        return earliestLeadSeconds / 3600.0;
    }

    /**
     * A convenience method that returns the latest forecast lead time in decimal hours. See also 
     * {@link #getLatestLeadTimeInSeconds()}.
     * 
     * @return the latest lead time in decimal hours
     */

    public double getLatestLeadTimeInDecimalHours()
    {
        return latestLeadSeconds / 3600.0;
    }

    /**
     * Hidden constructor.
     * 
     * @param earliestTime the earliest time
     * @param latestTime the latest time
     * @param validTime is true if the earliestTime and latestTime are in valid time, false for issue time
     * @param earliestLeadSeconds the earliest lead time in seconds
     * @param latestLeadSeconds the latest lead time in seconds
     * @throws IllegalArgumentException if the latestTime is before (i.e. smaller than) the earliestTime or the 
     *            latestLeadTime is before (i.e. smaller than) the earliestLeadTime.  
     */

    private TimeWindow( Instant earliestTime,
                        Instant latestTime,
                        boolean validTime,
                        int earliestLeadSeconds,
                        int latestLeadSeconds )
    {
        if ( latestTime.isBefore( earliestTime ) )
        {
            throw new IllegalArgumentException( "Cannot define a time window whose latest time is before its "
                                                + "earliest time." );
        }
        if ( latestLeadSeconds < earliestLeadSeconds )
        {
            throw new IllegalArgumentException( "Cannot define a time window whose latest lead time is before its "
                                                + "earliest lead time." );
        }
        this.earliestTime = earliestTime;
        this.latestTime = latestTime;
        this.validTime = validTime;
        this.earliestLeadSeconds = earliestLeadSeconds;
        this.latestLeadSeconds = latestLeadSeconds;
    }

}
