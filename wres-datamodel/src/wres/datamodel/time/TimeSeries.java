package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScale;

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
     * The {@link TimeScale} associated with the events in the time-series.
     */

    private final TimeScale timeScale;

    /**
     * Returns a {@link TimeSeries} with a reference time equal to the {@link Event#getTime()} of the first event or 
     * {@link Instant#MIN} when the input is empty. Assumes a type of {@link ReferenceTimeType#DEFAULT}. Also assumes
     * a time-scale of {@link TimeScale#of()}.
     *
     * @param <T> the event type
     * @param events the events
     * @return a time-series
     * @throws NullPointerException if the events are null or any one event is null
     */

    public static <T> TimeSeries<T> of( SortedSet<Event<T>> events )
    {
        return TimeSeries.of( Collections.emptyMap(), events );
    }

    /**
     * Returns a {@link TimeSeries} with a reference time type of {@link ReferenceTimeType#DEFAULT}. Assumes a 
     * time-scale of {@link TimeScale#of()}.
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
        return TimeSeries.of( Collections.singletonMap( ReferenceTimeType.DEFAULT, referenceTime ), events );
    }

    /**
     * Returns a {@link TimeSeries}.  Also assumes a time-scale of {@link TimeScale#of()}.
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
        return TimeSeries.of( Collections.singletonMap( referenceTimeType, referenceTime ), events );
    }

    /**
     * Returns a {@link TimeSeries}.  Also assumes a time-scale of {@link TimeScale#of()}.
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
        Objects.requireNonNull( referenceTimes );

        Objects.requireNonNull( events );

        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();
        events.forEach( builder::addEvent );
        referenceTimes.forEach( ( type, time ) -> builder.addReferenceTime( time, type ) );

        return builder.build();
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
     * Returns the {@link TimeScale} associated with the events.
     * 
     * @return the time-scale
     */

    public TimeScale getTimeScale()
    {
        return this.timeScale;
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
               && Objects.equals( this.getEvents(), input.getEvents() )
               && Objects.equals( this.getTimeScale(), input.getTimeScale() );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.getReferenceTimes(), this.getEvents(), this.getTimeScale() );
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ", ", "[", "]" );

        joiner.add( "TimeSeries@" + this.hashCode() );
        joiner.add( "Reference times: " + this.getReferenceTimes().toString() );
        joiner.add( "Events: " + this.getEvents() );
        joiner.add( "TimeScale: " + this.timeScale );

        return joiner.toString();
    }

    /**
     * Builds with a builder.
     * 
     * @param builder the builder
     */
    private TimeSeries( TimeSeriesBuilder<T> builder )
    {

        // Set then validate
        // Important to place in a new set here, because the builder uses a special 
        // comparator, based on event time only
        SortedSet<Event<T>>  localEvents = new TreeSet<>();
        localEvents.addAll( builder.events );
        this.events = Collections.unmodifiableSortedSet( localEvents );       

        Map<ReferenceTimeType, Instant> localMap = new TreeMap<>( builder.referenceTimes );

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

        if ( Objects.isNull( builder.timeScale ) )
        {
            this.timeScale = TimeScale.of();

            LOGGER.trace( "No time-scale information was provided for time-series {}: building a time-series with a "
                          + "default time-scale.",
                          this.hashCode() );
        }
        else
        {
            this.timeScale = builder.timeScale;
        }

        // All reference datetimes and types must be non-null
        for ( Map.Entry<ReferenceTimeType, Instant> nextEntry : this.getReferenceTimes().entrySet() )
        {
            Objects.requireNonNull( nextEntry.getKey() );

            Objects.requireNonNull( nextEntry.getValue() );
        }

        // No null events
        this.getEvents().forEach( Objects::requireNonNull );

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
         * The time-scale associated with the events.
         */

        private TimeScale timeScale;

        /**
         * Adds a reference time.
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
         * Adds a map of reference times.
         * 
         * @param referenceTimes the reference times
         * @return the builder
         * @throws NullPointerException if the input is null
         */

        public TimeSeriesBuilder<T> addReferenceTimes( Map<ReferenceTimeType, Instant> referenceTimes )
        {
            Objects.requireNonNull( referenceTimes );
            
            this.referenceTimes.putAll( referenceTimes );

            return this;
        }        

        /**
         * Adds several events to the time-series.
         * 
         * @param events the events
         * @return the builder
         * @throws IllegalArgumentException if the time-series already contains an event at the prescribed time
         */
        
        public TimeSeriesBuilder<T> addEvents( Set<Event<T>> events )
        {
            events.stream().forEach( this::addEvent );
            
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
         * Adds the time-scale information.
         * 
         * @param timeScale the time scale
         * @return the builder
         */

        public TimeSeriesBuilder<T> setTimeScale( TimeScale timeScale )
        {
            this.timeScale = timeScale;

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

