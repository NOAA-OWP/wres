package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A time-series contains a time-ordered set of {@link Event}, together with a reference datetime and associated 
 * {@link ReferenceTimeType}.
 * 
 * @param <T> the type of data
 * @author james.brown@hydrosolved.com
 */

public class TimeSeries<T>
{

    /**
     * The reference datetime associated with the time-series.
     */

    private final Instant referenceTime;

    /**
     * The type of reference time.
     */

    private final ReferenceTimeType referenceTimeType;

    /**
     * The events.
     */

    private final SortedSet<Event<T>> events;

    /**
     * Returns a {@link TimeSeries} with a reference time of {@link Instant#MIN} and a reference time type of
     * {@link ReferenceTimeType#UNKNOWN}.
     *
     * 
     * @param <T> the event type
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if events are null or any one event is null
     */

    public static <T> TimeSeries<T> of( SortedSet<Event<T>> events )
    {
        Instant referenceTime = Instant.MIN;

        if ( !events.isEmpty() )
        {
            referenceTime = events.first().getTime();
        }

        return TimeSeries.of( referenceTime, events );
    }

    /**
     * Returns a {@link TimeSeries} with a reference time type of {@link ReferenceTimeType#UNKNOWN}.
     * 
     * @param <T> the event type
     * @param referenceTime the reference time
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if events are null or any one event is null
     */

    public static <T> TimeSeries<T> of( Instant referenceTime,
                                         SortedSet<Event<T>> events )
    {
        return new TimeSeries<>( referenceTime, ReferenceTimeType.UNKNOWN, events );
    }
    
    /**
     * Returns a {@link TimeSeries}.
     * 
     * @param <T> the event type
     * @param referenceTime the reference time
     * @param referenceTimeType the type of reference time
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if events are null or any one event is null
     */

    public static <T> TimeSeries<T> of( Instant referenceTime,
                                         ReferenceTimeType referenceTimeType,
                                         SortedSet<Event<T>> events )
    {
        return new TimeSeries<>( referenceTime, referenceTimeType, events );
    }

    /**
     * Returns the underlying events in the time-series.
     * 
     * @return the underlying events.
     */

    public SortedSet<Event<T>> getEvents()
    {
        return this.events; // Rendered immutable on construction
    }

    /**
     * Returns the reference datetime.
     * 
     * @return the reference datetime
     */

    public Instant getReferenceTime()
    {
        return this.referenceTime;
    }

    /**
     * Returns the type of reference datetime.
     * 
     * @return the reference time type
     */

    public ReferenceTimeType getReferenceTimeType()
    {
        return this.referenceTimeType;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( Objects.isNull( o ) )
        {
            return false;
        }

        TimeSeries<?> input = (TimeSeries<?>) o;

        return Objects.equals( this.getReferenceTimeType(), input.getReferenceTimeType() )
               && Objects.equals( this.getReferenceTime(), input.getReferenceTime() )
               && Objects.equals( this.getEvents(), input.getEvents() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getReferenceTimeType(), this.getReferenceTime(), this.getEvents() );
    }

    /**
     * Build a time-series.
     * 
     * @param reference the reference datetime
     * @param referenceTimeType the type of reference time
     * @param events the events
     * @throws NullPointerException if the events are null or any individual event is null
     */

    private TimeSeries( Instant reference, ReferenceTimeType referenceTimeType, SortedSet<Event<T>> events )
    {
        Objects.requireNonNull( events );

        // No non-null events
        events.forEach( Objects::requireNonNull );

        this.events = Collections.unmodifiableSortedSet( events );

        this.referenceTime = reference;

        this.referenceTimeType = referenceTimeType;
    }

    /**
     * Builds with a builder.
     * 
     * @param builder the builder
     */
    private TimeSeries( TimeSeriesBuilder<T> builder )
    {
        this( builder.referenceTime, builder.referenceTimeType, builder.events );
    }

    /**
     * Builder that allows for incremental construction and validation. 
     * 
     * @param <T> the type of data
     */

    public static class TimeSeriesBuilder<T>
    {
        /**
         * Events with a prescribed comparator based on event time.
         */

        private final SortedSet<Event<T>> events =
                new TreeSet<>( ( e1, e2 ) -> e1.getTime().compareTo( e2.getTime() ) );

        /**
         * The reference datetime associated with the time-series.
         */

        private Instant referenceTime;

        /**
         * The type of reference time.
         */

        private ReferenceTimeType referenceTimeType;

        /**
         * Sets the reference time.
         * 
         * @param referenceTime the reference time
         * @return the builder
         */

        public TimeSeriesBuilder<T> setReferenceTime( Instant referenceTime )
        {
            this.referenceTime = referenceTime;

            return this;
        }

        /**
         * Sets the reference time type.
         * 
         * @param referenceTimeType the reference time type
         * @return the builder
         */

        public TimeSeriesBuilder<T> setReferenceTimeType( ReferenceTimeType referenceTimeType )
        {
            this.referenceTimeType = referenceTimeType;

            return this;
        }

        /**
         * Adds an event.
         * 
         * @param event the event
         * @return the builder
         * @throws IllegalArgumentException if the time-series already contains an event at the prescribed time
         */

        public TimeSeriesBuilder<T> addEvent( Event<T> event )
        {
            if ( this.events.contains( event ) )
            {
                throw new IllegalArgumentException( "Attemped to add an event at the same valid datetime as an "
                                                    + "existing event, which is not allowed. The duplicate event "
                                                    + "time is '"
                                                    + event.getTime()
                                                    + "'." );
            }

            this.events.add( event );

            return this;
        }

        /**
         * Builds the time-series.
         * 
         * @return a time-series
         */

        public TimeSeries<T> build()
        {
            return new TimeSeries<>( this );
        }

    }


}

