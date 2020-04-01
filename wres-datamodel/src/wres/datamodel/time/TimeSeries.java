package wres.datamodel.time;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import wres.datamodel.scale.TimeScale;

/**
 * <p>A time-series contains a time-ordered set of {@link Event}, together with zero or more reference datetimes and 
 * associated {@link ReferenceTimeType}.
 * 
 * <p><b>Implementation Notes:</b>
 * 
 * <p>This class is immutable and thread-safe.
 * 
 * @param <T> the type of time-series data
 * @author james.brown@hydrosolved.com
 * @author Jesse Bickel
 */

public class TimeSeries<T>
{
    /**
     * Any non-event-related metadata that apply to the time-series as a whole.
     */

    private final TimeSeriesMetadata metadata;


    /**
     * The events.
     */

    private final SortedSet<Event<T>> events;


    /**
     * Returns an empty {@link TimeSeries} with a <code>null</code> {@link TimeScale}.
     *
     * @param <T> the event type
     * @param metadata The metadata for the empty timeseries.
     * @return an empty time-series
     */

    public static <T> TimeSeries<T> of( TimeSeriesMetadata metadata )
    {
        return TimeSeries.of( metadata, Collections.emptySortedSet() );
    }

    /**
     * Returns a {@link TimeSeries} with prescribed metadata.
     * 
     * @param <T> the event type
     * @param timeSeriesMetadata the metadata
     * @param events the events
     * @return the time-series
     */

    public static <T> TimeSeries<T> of( TimeSeriesMetadata timeSeriesMetadata,
                                        SortedSet<Event<T>> events )
    {
        Objects.requireNonNull( timeSeriesMetadata );
        Objects.requireNonNull( events );

        return new TimeSeriesBuilder<T>().setMetadata( timeSeriesMetadata )
                                         .addEvents( events )
                                         .build();
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

    public TimeSeriesMetadata getMetadata()
    {
        return this.metadata;
    }

    /**
     * Returns the {@link TimeScale} associated with the events or <code>null</code> if {@link #hasTimeScale()} returns
     * <code>false</code>.
     * 
     * @return the time-scale or null
     */

    public TimeScale getTimeScale()
    {
        return this.getMetadata()
                   .getTimeScale();
    }

    /**
     * Returns <code>true</code> if the time-scale is known, otherwise <code>false</code>.
     * 
     * @return true if the time scale is known, otherwise false
     */

    public boolean hasTimeScale()
    {
        return Objects.nonNull( this.getMetadata() )
               && Objects.nonNull( this.getMetadata()
                                       .getTimeScale() );
    }

    /**
     * Returns the reference datetime.
     * 
     * @return the reference datetime
     */

    public Map<ReferenceTimeType, Instant> getReferenceTimes()
    {
        return this.getMetadata()
                   .getReferenceTimes(); //Rendered immutable on construction
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        TimeSeries<?> that = (TimeSeries<?>) o;
        return metadata.equals( that.metadata ) &&
               events.equals( that.events );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( metadata, events );
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                                                                            .append( "metadata", metadata )
                                                                            .append( "events", events )
                                                                            .toString();
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
        SortedSet<Event<T>> localEvents = new TreeSet<>();
        localEvents.addAll( builder.events );
        this.events = Collections.unmodifiableSortedSet( localEvents );

        TimeSeriesMetadata localMetadata = builder.metadata;
        if ( Objects.nonNull( localMetadata ) )
        {
            this.metadata = localMetadata;
        }
        // TODO: deprecated route, to remove
        // Once removed, require metadata
        else
        {
            throw new UnsupportedOperationException( "Use complete metadata in your TimeSeries instances." );
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
         * The time-series metadata.
         */

        private TimeSeriesMetadata metadata;

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
                throw new IllegalArgumentException( "Attempted to add an event at the same valid datetime as an "
                                                    + "existing event, which is not allowed. The duplicate event "
                                                    + "by time is '"
                                                    + event
                                                    + "'." );
            }

            this.events.add( event );

            return this;
        }

        /**
         * Sets the time-series metadata.
         * 
         * @param metadata the time-series metadata
         * @return the builder
         */

        public TimeSeriesBuilder<T> setMetadata( TimeSeriesMetadata metadata )
        {
            this.metadata = metadata;

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

