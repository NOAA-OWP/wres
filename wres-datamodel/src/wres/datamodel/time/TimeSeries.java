package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>A time-series contains a time-ordered set of {@link Event}, together with one or more reference datetimes and 
 * associated {@link ReferenceTimeType}.
 * 
 * <p><b>Implementation Notes:</b>
 * 
 * <p>This class is immutable and thread-safe.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 */

public class TimeSeries<T>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeries.class );

    /**
     * The one or more reference datetimes associated with the time-series.
     */

    private final Map<ReferenceTimeType, Instant> referenceTimes;

    /**
     * The events.
     */

    private final SortedSet<Event<T>> events;

    /**
     * Returns a {@link TimeSeries} with a reference time equal to the {@link Event#getTime()} of the first event or 
     * {@link Instant#MIN} when the input is empty. Assumes a type of {@link ReferenceTimeType#DEFAULT}.
     *
     * @param <T> the event type
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if the events are null or any one event is null
     */

    public static <T> TimeSeries<T> of( SortedSet<Event<T>> events )
    {
        return new TimeSeries<>( Collections.emptyMap(), events );
    }

    /**
     * Returns a {@link TimeSeries} with a reference time type of {@link ReferenceTimeType#DEFAULT}.
     * 
     * @param <T> the event type
     * @param referenceTime the reference time
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if any input is null or any individual event is null
     */

    public static <T> TimeSeries<T> of( Instant referenceTime,
                                        SortedSet<Event<T>> events )
    {
        return new TimeSeries<>( Collections.singletonMap( ReferenceTimeType.DEFAULT, referenceTime ), events );
    }

    /**
     * Returns a {@link TimeSeries}.
     * 
     * @param <T> the event type
     * @param referenceTime the reference time
     * @param referenceTimeType the type of reference time
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if any input is null or any individual event is null
     */

    public static <T> TimeSeries<T> of( Instant referenceTime,
                                        ReferenceTimeType referenceTimeType,
                                        SortedSet<Event<T>> events )
    {
        return new TimeSeries<>( Collections.singletonMap( referenceTimeType, referenceTime ), events );
    }
    
    /**
     * Returns a {@link TimeSeries}.
     * 
     * @param <T> the event type
     * @param referenceTimes the reference times
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if any input is null or any individual event is null
     */

    public static <T> TimeSeries<T> of( Map<ReferenceTimeType, Instant> referenceTimes,
                                        SortedSet<Event<T>> events )
    {
        return new TimeSeries<>( referenceTimes, events );
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

    public Map<ReferenceTimeType, Instant> getReferenceTimes()
    {
        return this.referenceTimes; //Rendered immutable on construction
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof TimeSeries<?> ) )
        {
            return false;
        }

        TimeSeries<?> input = (TimeSeries<?>) o;

        return Objects.equals( this.getReferenceTimes(), input.getReferenceTimes() )
               && Objects.equals( this.getEvents(), input.getEvents() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getReferenceTimes(), this.getEvents() );
    }

    /**
     * Build a time-series.
     * 
     * @param referenceTimes the reference datetimes associated with the time-series
     * @param events the events
     * @throws NullPointerException if any input is null or any individual event is null
     */

    private TimeSeries( Map<ReferenceTimeType, Instant> referenceTimes, SortedSet<Event<T>> events )
    {
        // Set then validate, as this class includes a mutable builder
        // as a possible source of input
        this.events = Collections.unmodifiableSortedSet( events );

        Map<ReferenceTimeType, Instant> localMap = new TreeMap<>( referenceTimes );

        // Add a default reference time if needed
        if ( localMap.isEmpty() )
        {
            Instant defaultTime = Instant.MIN;
            ReferenceTimeType defaultType = ReferenceTimeType.DEFAULT;

            if ( !this.getEvents().isEmpty() )
            {
                defaultTime = this.getEvents().first().getTime();
            }

            localMap.put( defaultType, defaultTime );

            LOGGER.trace( "Added a default reference time of {} and type {} for time-series {}.",
                          defaultTime,
                          defaultType,
                          this.hashCode() );
        }

        this.referenceTimes = Collections.unmodifiableMap( localMap );

        // All reference datetimes and types must be non-null
        for ( Map.Entry<ReferenceTimeType, Instant> nextEntry : this.getReferenceTimes().entrySet() )
        {
            Objects.requireNonNull( nextEntry.getKey() );

            Objects.requireNonNull( nextEntry.getValue() );
        }

        // No null collection of events
        Objects.requireNonNull( this.getEvents() );

        // No null events
        this.getEvents().forEach( Objects::requireNonNull );
    }

    /**
     * Builds with a builder.
     * 
     * @param builder the builder
     */
    private TimeSeries( TimeSeriesBuilder<T> builder )
    {
        this( builder.referenceTimes, builder.events );
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

        private final Map<ReferenceTimeType, Instant> referenceTimes = new TreeMap<>();

        /**
         * Sets the reference time.
         * 
         * @param referenceTime the reference time
         * @param referenceTimeType the reference time type
         * @return the builder
         */

        public TimeSeriesBuilder<T> addReferenceTime( Instant referenceTime, ReferenceTimeType referenceTimeType )
        {
            this.referenceTimes.put( referenceTimeType, referenceTime );

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

